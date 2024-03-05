Python exchange connector for thalex.

thalex.py has a function for every websocket endpoint with typehints and a recieve function 
that returns the messages from the exchange one by one.

The example_options.py and example_rolls.py are examples of how you might want to quote those instruments.

example_taker.py is a simple taker for one instrument that keeps trying to take with a defined pnl,
until a desired position is entered into.

Keep in mind that the examples are not meant to be out of the box money printers, 
they just illustrate what an implementation of a trading strategy could look like.

If you want to run the examples, you have to rename/copy _keys.py to keys.py, 
create api keys on thalex ui and put them in keys.py.

Feel free to use it however you want, just remember that it comes without warranty.

If you spot any errors/bugs please report or create a pull request.
