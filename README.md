This is a free to use python exchange connector library for [thalex](https://www.thalex.com).

See also the [api documentation](https://www.thalex.com/docs/).

thalex.py has a function for every websocket endpoint with typehints and a recieve function 
that returns the messages from the exchange one by one.

There are some examples on how you could use this library:
- example_instrument_counter.py is a very simple example that prints how many different there are on prod
- example_perp.py, example_options.py and example_rolls.py are examples of how you might want to use the library to
quote those instruments.
- example_taker.py is the most straightforward example. It shows how you could use this library to implement a 
simple taker that keeps taking with a set minimum pnl until a desired position is achieved.

Keep in mind that the examples are not meant to be out of the box money printers, 
they just illustrate what an implementation of a trading bot could look like.

If you want to run the examples, you have to rename/copy _keys.py to keys.py, 
create api keys on thalex ui and put them in keys.py.

Feel free to use it however you want, just remember that it comes without warranty.

If you spot any errors/bugs please report or create a pull request.
