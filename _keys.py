import thalex_py

# You have to create api keys on thalex ui.
# TEST: https://testnet.thalex.com/exchange/user/api
# PROD: https://thalex.com/exchange/user/api
# If you don't want to quote on test/prod you can just leave the
# corresponding key / key_id as it is.
private_keys = {
    thalex_py.Network.TEST: """-----BEGIN RSA PRIVATE KEY-----
    ...
-----END RSA PRIVATE KEY-----
""",
    thalex_py.Network.PROD: """-----BEGIN RSA PRIVATE KEY-----
    ...
-----END RSA PRIVATE KEY-----
""",
}

key_ids = {
    thalex_py.Network.TEST: "K12345679",
    thalex_py.Network.PROD: "K12345679",
}
