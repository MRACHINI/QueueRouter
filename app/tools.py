"""Small tools QueueRouter runs itself for the orchestrator (see app/council.py): a calculator, and today's
date (also written into its instructions). Server-side and harmless by design: no code, no files."""

from __future__ import annotations

import ast
import math
import operator
import time

CALC_NAME = "queuerouter_calculate"
CALC_TOOL = {
    "type": "function",
    "function": {
        "name": CALC_NAME,
        "description": "Calculate a maths expression exactly, e.g. 262144*0.8 or (3.5-1.2)/7 or sqrt(2)*log(10).",
        "parameters": {
            "type": "object",
            "properties": {"expression": {"type": "string"}},
            "required": ["expression"],
        },
    },
}
_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}
_FUNCS = {
    name: getattr(math, name)
    for name in ("sqrt", "log", "log2", "log10", "exp", "sin", "cos", "tan", "floor", "ceil", "fabs", "factorial")
} | {"abs": abs, "round": round, "min": min, "max": max}
_CONSTS = {"pi": math.pi, "e": math.e}
_ALLOWED = "only numbers, + - * / // % **, parentheses, pi, e and maths functions are allowed"


def _value(node):
    if isinstance(node, ast.Expression):
        return _value(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, int | float) and not isinstance(node.value, bool):
        return node.value
    if isinstance(node, ast.Name) and node.id in _CONSTS:
        return _CONSTS[node.id]
    if isinstance(node, ast.UnaryOp) and type(node.op) in _OPS:
        return _OPS[type(node.op)](_value(node.operand))
    if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
        left, right = _value(node.left), _value(node.right)
        if isinstance(node.op, ast.Pow) and (abs(right) > 1000 or abs(left) > 1e12):
            raise ValueError("power too large")
        return _OPS[type(node.op)](left, right)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _FUNCS and not node.keywords:
        arguments = [_value(a) for a in node.args]
        if node.func.id == "factorial" and (not arguments or arguments[0] > 1000):
            raise ValueError("factorial too large")
        return _FUNCS[node.func.id](*arguments)
    raise ValueError(_ALLOWED)


def calculate(expression: str) -> str:
    """The value of a maths expression, or why it can't be calculated (never runs code)."""
    expression = str(expression or "").strip()[:500]
    try:
        result = _value(ast.parse(expression, mode="eval"))
    except (SyntaxError, ValueError, TypeError, ZeroDivisionError, OverflowError) as exc:
        return f"Can't calculate {expression!r}: {exc}"
    if isinstance(result, float) and result.is_integer() and abs(result) < 1e15:
        result = int(result)
    return f"{expression} = {result}"


def today() -> str:
    return time.strftime("%A %Y-%m-%d, %H:%M UTC", time.gmtime())
