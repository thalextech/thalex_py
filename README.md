Python exchange connector for thalex.

thalex.py has a function for every websocket endpoint with typehints and a recieve function that returns the messages from the exchange one by one.

example_trader.py has a minimalistic example of how the connector can be used.
If you want to use it, you have to rename/copy _keys.py to keys.py, create api
keys on thalex ui and put them in keys.py.

process_msg,py is a little helper to process messages from thalex.

Feel free to use it however you want, just remember that it comes without warranty.

If you spot any errors/bugs please report or create a pull request.
