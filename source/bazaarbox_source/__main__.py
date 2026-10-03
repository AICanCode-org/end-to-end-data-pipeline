"""python -m bazaarbox_source <generate|serve> ..."""

import sys

from . import api, generate


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("generate", "serve"):
        print("usage: python -m bazaarbox_source <generate|serve> [--help]", file=sys.stderr)
        return 2
    cmd, rest = sys.argv[1], sys.argv[2:]
    return generate.main(rest) if cmd == "generate" else api.main(rest)


if __name__ == "__main__":
    raise SystemExit(main())
