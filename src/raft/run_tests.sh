for i in {0..50}
do
    go test -race -run 2A > "test_output-$i" &
done

wait
