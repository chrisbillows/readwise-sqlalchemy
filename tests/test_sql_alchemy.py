import pytest



@pytest.mark.parametrize("attribute, check_function", 
    [
        ("APPLICATION_DIR", lambda attribute: attribute.is_dir()), 
        ("ENV_FILE", lambda attribute: attribute.exists()), 
        ("READWISE_API_TOKEN", lambda attribute: attribute == "abc123")
    ]
)
def test_init(temp_user_config, attribute, check_function):
    actual = getattr(temp_user_config, attribute)
    assert check_function(actual)



