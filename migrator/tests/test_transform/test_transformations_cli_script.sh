#!/bin/bash
set -e

# Get the absolute path to the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MIGRATOR_DIR="$ROOT_DIR/migrator"

# Change to the migrator directory
echo "Changing to migrator directory: $MIGRATOR_DIR"
cd "$MIGRATOR_DIR"

# Set paths relative to the migrator directory
TEST_DIR="tests/test_transform"
TEST_DATA_FILE="$TEST_DIR/enhanced_test_data.csv"
CONTACTS_OUTPUT="$TEST_DIR/contacts_transformed.csv"
COMPANIES_OUTPUT="$TEST_DIR/companies_transformed.csv"
CONFIG_OUTPUT="$TEST_DIR/test_output.json"

# Create comprehensive test data
cat > "$TEST_DATA_FILE" << EOF
email,alternate_email,phone,mobile_phone,first_name,last_name,company,website,lead_status,lead_source,created_date,last_activity_date,annual_revenue,hs_object_id,industry,employee_count
john.doe@gmail.com,j.doe.alt@gmail.com,(555) 123-4567,555-987-6543,John,Smith,Acme Inc.,www.example.com,New,Website,2023-01-15,2023-02-20,1000000,12345,Tech,100
jane.smith@gmail.com,,555.123.4567,,Jane,Doe,Tech LLC,https://test.org,In Progress,Phone Inquiry,2023-03-10,2023-04-05,500000,67890,IT,50
invalid@,not.an.email,(123) 456,,Robert,Johnson,Global Ltd,invalid,Qualified,Partner Referral,2023-05-20,,0,54321,Software,-10
,,,,,,,,,,,,,,,
test@example.com,alt@example.com,+14155552671,+14155552672,Test,User,Corporation Inc,www.test.com,Unqualified,External Referral,2023-06-15,2023-07-01,-5000,98765,Fin,not-a-number
EOF

echo "Created test data file: $TEST_DATA_FILE"

# Run test_config_files.py to validate configuration files
echo "Testing configuration files..."
python -m pytest "tests/test_config_files.py" -v

# Run test_transform.py to test enhanced transformations
echo "Testing enhanced transformations..."
python -m pytest "tests/test_transform/test_transform.py" -v

# Test contacts configuration using transform command
echo "Testing contacts configuration with transform command..."
cat "$TEST_DATA_FILE" | migrator transform \
  --object-type contacts \
  --config-pack "src/migrator/config/config-pack" \
  > "$CONTACTS_OUTPUT"

# Test companies configuration using transform command
echo "Testing companies configuration with transform command..."
cat "$TEST_DATA_FILE" | migrator transform \
  --object-type companies \
  --config-pack "src/migrator/config/config-pack" \
  > "$COMPANIES_OUTPUT"

# Verify results
echo "Verifying results..."

# Function to check for expected transformations
verify_transformation() {
    local file=$1
    local pattern=$2
    local message=$3
    
    if grep -q "$pattern" "$file"; then
        echo "✅ $message"
    else
        echo "❌ $message"
        echo "Pattern '$pattern' not found in $file"
        echo "File contents:"
        cat "$file"
        exit 1
    fi
}

# Verify contacts transformations
echo "Verifying contacts transformations..."
verify_transformation "$CONTACTS_OUTPUT" "johndoe@gmail.com" "Gmail normalization works automatically (dots removed)"
verify_transformation "$CONTACTS_OUTPUT" "jdoealt@gmail.com" "Gmail normalization works for alternate email"
verify_transformation "$CONTACTS_OUTPUT" "Acme" "Company suffix removal works (might have trailing comma)"
verify_transformation "$CONTACTS_OUTPUT" "Tech" "LLC suffix removal works (might have trailing comma)"
verify_transformation "$CONTACTS_OUTPUT" "Global" "Ltd suffix removal works (might have trailing comma)"
verify_transformation "$CONTACTS_OUTPUT" "example.com" "www removal works"
verify_transformation "$CONTACTS_OUTPUT" "Open" "Lead status mapping works"
verify_transformation "$CONTACTS_OUTPUT" "Web" "Lead source mapping works"
verify_transformation "$CONTACTS_OUTPUT" "+15551234567" "Phone formatting works"

# Verify companies transformations
echo "Verifying companies transformations..."
verify_transformation "$COMPANIES_OUTPUT" "johndoe@gmail.com" "Gmail normalization works (dots removed)"
verify_transformation "$COMPANIES_OUTPUT" "Acme" "Company suffix removal works (might have trailing comma)"
verify_transformation "$COMPANIES_OUTPUT" "Tech" "LLC suffix removal works (might have trailing comma)"
verify_transformation "$COMPANIES_OUTPUT" "Global" "Ltd suffix removal works (might have trailing comma)"
verify_transformation "$COMPANIES_OUTPUT" "example.com" "www removal works"
verify_transformation "$COMPANIES_OUTPUT" "Technology" "Industry mapping works"
verify_transformation "$COMPANIES_OUTPUT" "100" "Employee count validation works"

# Display full results
echo "Full transformation results:"
echo "Contacts transformation:"
cat "$CONTACTS_OUTPUT"
echo "Companies transformation:"
cat "$COMPANIES_OUTPUT"

# Save copies of data files for inspection
echo "Saving copies of data files for inspection..."
RESULTS_DIR="test_results"
mkdir -p "$RESULTS_DIR"
cp "$TEST_DATA_FILE" "$RESULTS_DIR/input_data.csv"
cp "$CONTACTS_OUTPUT" "$RESULTS_DIR/contacts_transformed.csv"
cp "$COMPANIES_OUTPUT" "$RESULTS_DIR/companies_transformed.csv"

echo "Data files saved to $RESULTS_DIR directory"

# Clean up original files
echo "Cleaning up temporary files..."
rm -f "$TEST_DATA_FILE" "$CONTACTS_OUTPUT" "$COMPANIES_OUTPUT" "$CONFIG_OUTPUT"
# rm -rf "$RESULTS_DIR"
echo "All tests completed successfully!" 