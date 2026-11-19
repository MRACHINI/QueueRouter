"""Speech on Qwen Cloud (app/media.py): OpenAI's /v1/audio/speech and /v1/audio/transcriptions (an upload too)
translated to Qwen's own APIs, the characters spoken and seconds heard on the request and in the usage totals, and the
same through /mcp/media's text_to_speech and transcribe."""

import base64
import re

import httpx

from app import media
from tests.conftest import ACCESS_KEY, eventually
from tests.test_mcp import call
from tests.test_media import qwen
from tests.test_usage import flushed

TTS, ASR = "qwen-audio-3.0-tts-plus", "qwen-audio-3.0-asr-flash"


def test_speech_requests_in_qwens_form():
    assert media.speech_request({"model": TTS, "input": "Hi", "voice": "alloy"}) == {
        "model": TTS,
        "input": {"text": "Hi", "voice": media.DEFAULT_VOICE, "format": "mp3"},
    }
    own = media.speech_request({"model": TTS, "input": "Hi", "voice": "longanlingxin", "response_format": "wav"})
    assert own["input"]["voice"] == "longanlingxin" and own["input"]["format"] == "wav"
    assert media.speech_request({"model": TTS, "input": "Hi", "response_format": "flac"})["input"]["format"] == "mp3"
    heard = media.transcription_request(
        {"model": ASR, "file": "data:audio/wav;base64,AAAA", "language": "en", "prompt": "a podcast"}
    )
    assert heard["parameters"] == {"format": "wav", "language_hints": ["en"]}
    assert heard["input"]["messages"][0] == {"role": "system", "content": [{"text": "a podcast"}]}
    assert heard["input"]["messages"][1]["content"] == [
        {"type": "input_audio", "input_audio": {"data": "data:audio/wav;base64,AAAA"}}
    ]
    assert media.transcription_request({"model": ASR, "file": "https://x/a.m4a?sig=1"})["parameters"]["format"] == "m4a"
    assert media.audio_kind(TTS) == "speech" and media.audio_kind(ASR) == "transcription"
    assert media.audio_kind("qwen-audio-3.0-realtime-plus") is None and media.audio_kind("qwen4-max") is None
    assert media.price_text(TTS, "speech") == "$0.2 per 10,000 characters"


def test_an_upload_as_json():
    boundary = "XyZ"
    body = (
        (
            f'--{boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\n{ASR}\r\n'
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="talk.mp3"\r\n'
            "Content-Type: audio/mpeg\r\n\r\n"
        ).encode()
        + b"\x00\x01binary"
        + f"\r\n--{boundary}--\r\n".encode()
    )
    fields = media.form_fields(body, f"multipart/form-data; boundary={boundary}")
    assert fields["model"] == ASR and fields["format"] == "mp3"
    assert fields["file"] == "data:audio/mpeg;base64," + base64.b64encode(b"\x00\x01binary").decode()
    assert media.form_fields(b"{}", "application/json") is None


async def test_speech_through_queuerouter(make_stack):
    stack = make_stack(models_cache_s=0)
    qwen(stack, TTS, ASR)
    store = stack.app.state.store
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        await c.get("/v1/models", headers=stack.headers)  # the speech models are listed
        r = await c.post(
            "/v1/audio/speech", headers=stack.headers, json={"model": TTS, "input": "Hello there", "voice": "nova"}
        )
        assert r.status_code == 200, r.text
        assert r.headers["content-type"] == "audio/mpeg" and r.content.startswith(b"ID3")
        assert stack.mock["qwen_speech_requests"][-1]["input"] == {
            "text": "Hello there",
            "voice": media.DEFAULT_VOICE,
            "format": "mp3",
        }

        # the upload OpenAI's SDKs send, turned into the JSON Qwen takes
        files = {"file": ("talk.mp3", b"\x00\x01recording", "audio/mpeg")}
        r = await c.post(
            "/v1/audio/transcriptions", headers=stack.headers, files=files, data={"model": ASR, "language": "en"}
        )
        assert r.status_code == 200, r.text
        assert r.json()["text"] == "hello from the recording" and r.json()["usage"]["seconds"] == 3.5
        sent = stack.mock["qwen_asr_requests"][-1]
        assert sent["parameters"] == {"format": "mp3", "language_hints": ["en"]}
        part = sent["input"]["messages"][-1]["content"][0]["input_audio"]["data"]
        assert part == "data:audio/mpeg;base64," + base64.b64encode(b"\x00\x01recording").decode()
        r = await c.post(
            "/v1/audio/transcriptions",
            headers=stack.headers,
            files=files,
            data={"model": ASR, "response_format": "text"},
        )
        assert r.status_code == 200 and r.text == "hello from the recording"

        await flushed(stack, 3)
        rows = [r for r in store.list_requests(10) if r["speech_characters"] or r["audio_seconds"]]
        spoken = next(r for r in rows if r["speech_characters"])
        assert spoken["speech_characters"] == 11 and spoken["media_units"] == "11 characters"
        assert round(spoken["media_cost"], 6) == 0.00022
        heard = [r for r in rows if r["audio_seconds"]]
        assert len(heard) == 2 and heard[0]["media_units"] == "3.5 s of audio"
        assert round(heard[0]["media_cost"], 7) == 0.0001225

        def totals():
            return tuple(store.query("SELECT SUM(speech_characters), SUM(audio_seconds) FROM usage_daily")[0])

        await eventually(lambda: totals() == (11, 7.0), timeout=10)

        # through /mcp/media
        async def tool(name: str, **args) -> dict:
            r = await c.post(f"/mcp/media/{ACCESS_KEY}", json=call("tools/call", {"name": name, "arguments": args}))
            assert r.status_code == 200, r.text
            return r.json()["result"]

        listed = (await tool("list_media_models"))["content"][0]["text"]
        assert f"{TTS} (speech): $0.2 per 10,000 characters" in listed and f"{ASR} (transcription)" in listed
        spoken = await tool("text_to_speech", text="Hi")
        assert spoken["content"][0]["type"] == "audio" and spoken["content"][0]["mimeType"] == "audio/mpeg"
        said = spoken["content"][1]["text"]
        assert said.startswith(f"Spoke 2 characters with {TTS}, $0.00004. Download: ")
        link = re.search(r"https?://\S+/media/\S+", said)[0]
        await eventually(
            lambda: any(store.query("SELECT 1 FROM media_files WHERE kind = 'audio' AND file IS NOT NULL"))
        )
        got = await c.get(link)
        assert got.status_code == 200 and got.content.startswith(b"ID3")
        heard = (await tool("transcribe", audio="https://example.com/talk.wav", language="en"))["content"][0]["text"]
        assert heard.startswith(f"Transcribed 3.5 s of audio with {ASR}, $0.00012")  # 3.5 s at $0.000035
        assert heard.endswith(":\n\nhello from the recording")
        assert stack.mock["qwen_asr_requests"][-1]["parameters"]["format"] == "wav"
