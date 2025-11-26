
current_dir=$(pwd)
echo "Current dir is" $current_dir

"$current_dir/scripts/data/populate_customers.sh"
"$current_dir/scripts/data/populate_flights.sh"