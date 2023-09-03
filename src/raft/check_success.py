import os

TEST_DIR = "./test_output"

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


for filename in os.listdir(TEST_DIR):
    if filename.startswith("test_output-"):
        failed_tests = find_failed_tests(filename)
        if len(failed_tests) != 0:
            print("Contains failed tests: ", filename)
            print("Tests failed:", ", ".join(failed_tests))
            print()

print("Done")

