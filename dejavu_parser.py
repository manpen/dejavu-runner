from pathlib import Path
import re


def strip_ansi(text: str) -> str:
    ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ANSI_ESCAPE.sub("", text)


def load_dejavu_log(path: Path):
    PARSE_TIME = re.compile(
        r"c parse_time=(\d+\.?\d*)ms, file_size=(\d+\.?\d*)mb, n=(\d+), m=(\d+)"
    )
    num = r"\s+(\d+\.?\d*|inf|-?nan)"
    PREPROC_LINE = re.compile("c" + num + r"\s+(\w+)" + (num * 10) + "%")
    TIME_LINE = re.compile(f"c{num}ms{num}%\\s(\\w+)")
    FINAL_LINE = re.compile(r"c solve_time=(\d+\.?\d*)ms")

    result = {"preproc": {}, "preproc_rows": [],  "times": {}}

    stage = 0

    with open(path) as f:
        for line in f:
            line = strip_ansi(line).strip()

            m = PARSE_TIME.match(line)
            if m:
                assert stage == 0
                stage = 1
                result["parse_time"] = float(m.group(1))
                result["file_size"] = float(m.group(2))
                result["n"] = int(m.group(3))
                result["m"] = int(m.group(4))
                continue

            m = PREPROC_LINE.match(line)
            if m:
                assert stage <= 2
                stage = 2
                row = {
                    "time": float(m.group(1)),
                    "routine": m.group(2),
                    "cref": float(m.group(3)),
                    "csch": float(m.group(4)),
                    "n": int(m.group(5)),
                    "m": int(m.group(6)),
                    "ref": float(m.group(7)),
                    "comp": float(m.group(8)),
                    "tcomp": float(m.group(9)),
                    "restart": int(m.group(10)),
                    "leaves": int(m.group(11)),
                    "est": float(m.group(12)),
                    "position": len(result["preproc"]),
                }
                result["preproc"][m.group(2)] = row
                result["preproc_rows"].append(row)

                continue

            m = TIME_LINE.match(line)
            if m:
                assert stage <= 3
                stage = 3

                result["times"][m.group(3)] = {
                    "routine": m.group(3),
                    "time": float(m.group(1)),
                    "frac": float(m.group(2)),
                }

            m = FINAL_LINE.match(line)
            if m:
                stage = 1000
                result["solve_time"] = float(m.group(1))

    if len(result["preproc_rows"]) > 0:
        result["final_n"] = result["preproc_rows"][-1]["n"]
        result["final_m"] = result["preproc_rows"][-1]["m"]

    return result


if __name__ == "__main__":
    load_dejavu_log(Path("logs/large.dimacs.stderr"))
