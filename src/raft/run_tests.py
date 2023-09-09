import argparse
import subprocess
import os


tags = ["2A", "2B", "2C", "2D"]
TEST_DIR = "./test_output"
TEST_FILENAME_PREFIX = "test_output-"

def parse_args():
    def testtag(tag: str):
        if any([tag.endswith(suffix) for suffix in tags]):
            return tag
        raise argparse.ArgumentTypeError("Expected the test tag to be one of %s" % ", ".join(tags))

    parser = argparse.ArgumentParser(usage="%(prog)s [OPTION] [TEST TAG]", description="Run the raft tests")
    parser.add_argument("--testtag", "-t", type=testtag)
    parser.add_argument("--race", action="store_true", help="Should the race flag be used in running the tests?")
    parser.add_argument("--all", action="store_true", help="Should all the tests be run?")
    args = parser.parse_args()
    if args.testtag is None and not args.all:
        args.all = True
    return args

def run_test(tag: str, race: bool, test_no: int):
    out_filename = f"{TEST_DIR}/{TEST_FILENAME_PREFIX}{test_no}"
    os.makedirs(os.path.dirname(out_filename), exist_ok=True)
    with open(out_filename, "w") as f:
        subprocess.run(["go", "test", "-race" if race else "", "-run", tag], stdout=f, stderr=f)

def find_failed_tests(filename):
    failed_tests = []
    with open(f"{TEST_DIR}/{filename}") as f:
        lines = f.read().splitlines()
        last_line = lines[len(lines) - 1]
        if not last_line.startswith("ok"):
            for line in lines:
                if line.startswith("--- FAIL:"):
                    test_name = line.split("--- FAIL:")[1]
                    failed_tests.append(test_name)
    return failed_tests

def check_test_outputs() -> bool:
    some_failures = False
    for filename in os.listdir(TEST_DIR):
        if filename.startswith(TEST_FILENAME_PREFIX):
            failed_tests = find_failed_tests(filename)
            if len(failed_tests) != 0:
                some_failures = True or some_failures
                print("Contains failed tests: ", filename)
                print("Tests failed:", ", ".join(failed_tests))
                print()
    return some_failures

def main():
    args = parse_args()
    t = tags if args.all else [args.testtag]
    for tag in t:
        for n in range(10):
            run_test(tag, args.race, n)
        some_failed = check_test_outputs()
        if some_failed:
            break
        subprocess.run(["rm", "-r", TEST_DIR])
        print("Done with tests", tag)

main()
