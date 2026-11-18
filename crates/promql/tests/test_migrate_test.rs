//! Port of `promql/promqltest/test_migrate_test.go`.

use promql::promqltest::migrate::migrate_test_data;

fn assert_migration(mode: &str, input: &str, expected: &str) {
    let dir = std::env::temp_dir().join(format!(
        "promqltest-migrate-{}-{}",
        std::process::id(),
        mode
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let test_file = dir.join("testcase.test");
    std::fs::write(&test_file, input).unwrap();
    migrate_test_data(mode, dir.to_str().unwrap()).unwrap();
    let output = std::fs::read_to_string(&test_file).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(expected, output);
}

#[test]
fn test_migrate_test_data_basic_mode() {
    let tests: Vec<(&str, &str, &str)> = vec![
        (
            "Basic mode with fail",
            r####"
				eval_fail instant at 1m sum(foo)
				  expected_fail_message something went wrong
				  {src="a"} 1
			"####,
            r####"
				eval instant at 1m sum(foo)
				  expect fail msg:something went wrong
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with warn",
            r####"
				eval_warn instant at 2m avg(bar)
				  {src="a"} 1
			"####,
            r####"
				eval instant at 2m avg(bar)
				  expect warn
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with info",
            r####"
				eval_info instant at 3m min(baz)
				  {src="a"} 1
			"####,
            r####"
				eval instant at 3m min(baz)
				  expect info
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with ordered",
            r####"
				eval_ordered instant at 4m max(qux)
				  {src="a"} 1
			"####,
            r####"
				eval instant at 4m max(qux)
				  expect ordered
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with multiple eval blocks",
            r####"
				eval_fail instant at 1m sum(foo)
				  expected_fail_message something else went wrong
				  {src="a"} 1

				eval_warn instant at 2m avg(bar)
				  {src="a"} 1
			"####,
            r####"
				eval instant at 1m sum(foo)
				  expect fail msg:something else went wrong
				  {src="a"} 1

				eval instant at 2m avg(bar)
				  expect warn
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with already migrated syntax (no changes)",
            r####"
				eval instant at 1m sum(foo)
				  expect fail msg:something went wrong
				  {src="a"} 1
			"####,
            r####"
				eval instant at 1m sum(foo)
				  expect fail msg:something went wrong
				  {src="a"} 1
			"####,
        ),
        (
            "Basic mode with only comments and whitespace",
            r####"
			# This is a comment
			
			"####,
            r####"
			# This is a comment
			
			"####,
        ),
    ];
    for (_name, input, expected) in tests {
        assert_migration("basic", input, expected);
    }
}

#[test]
fn test_migrate_test_data_strict_mode() {
    let tests: Vec<(&str, &str, &str)> = vec![
        (
            "Strict mode with fail",
            r####"
				eval_fail instant at 1m sum(foo)
					expected_fail_message something went wrong
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					expect no_warn
					expect no_info
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with warn",
            r####"
				eval_warn instant at 2m avg(bar)
					{src="a"} 1
"####,
            r####"
				eval instant at 2m avg(bar)
					expect warn
					expect no_info
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with info",
            r####"
				eval_info instant at 3m min(baz)
					{src="a"} 1
"####,
            r####"
				eval instant at 3m min(baz)
					expect info
					expect no_warn
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with ordered",
            r####"
				eval_ordered instant at 4m max(qux)
					{src="a"} 1
"####,
            r####"
				eval instant at 4m max(qux)
					expect ordered
					expect no_warn
					expect no_info
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with multiple eval blocks",
            r####"
				eval_fail instant at 1m sum(foo)
					expected_fail_message something else went wrong
					{src="a"} 1

				eval_warn instant at 2m avg(bar)
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something else went wrong
					expect no_warn
					expect no_info
					{src="a"} 1

				eval instant at 2m avg(bar)
					expect warn
					expect no_info
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with already migrated syntax (no changes)",
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					expect no_warn
					expect no_info
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					expect no_warn
					expect no_info
					{src="a"} 1
"####,
        ),
        (
            "Strict mode with only comments and whitespace",
            r####"
			# This is a comment

"####,
            r####"
			# This is a comment

"####,
        ),
    ];
    for (_name, input, expected) in tests {
        assert_migration("strict", input, expected);
    }
}

#[test]
fn test_migrate_test_data_tolerant_mode() {
    let tests: Vec<(&str, &str, &str)> = vec![
        (
            "Tolerant mode with fail",
            r####"
				eval_fail instant at 1m sum(foo)
					expected_fail_message something went wrong
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with warn",
            r####"
				eval_warn instant at 2m avg(bar)
					{src="a"} 1
"####,
            r####"
				eval instant at 2m avg(bar)
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with info",
            r####"
				eval_info instant at 3m min(baz)
					{src="a"} 1
"####,
            r####"
				eval instant at 3m min(baz)
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with ordered",
            r####"
				eval_ordered instant at 4m max(qux)
					{src="a"} 1
"####,
            r####"
				eval instant at 4m max(qux)
					expect ordered
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with multiple eval blocks",
            r####"
				eval_fail instant at 1m sum(foo)
					expected_fail_message something else went wrong
					{src="a"} 1

				eval_warn instant at 2m avg(bar)
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something else went wrong
					{src="a"} 1

				eval instant at 2m avg(bar)
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with already migrated syntax (no changes)",
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					{src="a"} 1
"####,
            r####"
				eval instant at 1m sum(foo)
					expect fail msg:something went wrong
					{src="a"} 1
"####,
        ),
        (
            "Tolerant mode with only comments and whitespace",
            r####"
			# This is a comment

"####,
            r####"
			# This is a comment

"####,
        ),
    ];
    for (_name, input, expected) in tests {
        assert_migration("tolerant", input, expected);
    }
}
