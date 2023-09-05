USAGE="Usage: run_tests.sh [test tag] [--race]"

if [ $# -ne 1 ] && [ $# -ne 2 ]
then
    echo "$USAGE"
    exit 1
fi

if [ $# -eq 2 ] && [ $2 != "--race" ]
then
    echo "$USAGE"
    exit 1
fi

mkdir test_output &> /dev/null

for i in {0..9}
do
    if [ $# -eq 2 ]
    then
        go test -race -run $1 > "test_output/test_output-$i" 
    else
        go test -run $1 > "test_output/test_output-$i" 
    fi
done

