import thalex_py

private_keys = {
    thalex_py.Network.TEST: """-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----
""",
    thalex_py.Network.PROD: """-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----
"""
}

key_ids = {
    thalex_py.Network.TEST: "K123456789",
    thalex_py.Network.PROD: "K123456789"
}
