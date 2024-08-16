This is a free to use python exchange connector library for [thalex](https://www.thalex.com).

See also the [api documentation](https://www.thalex.com/docs/).

thalex.py has a function for every websocket endpoint with typehints and a recieve function 
that returns the messages from the exchange one by one.

There are some examples on how you could use this library in the examples folder.

Keep in mind that the examples are not meant to be financial advice,
they just illustrate what an implementation of a trading bot could look like.

If you want to run the examples, you have to rename/copy _keys.py to keys.py, 
create api keys on thalex ui and put them in keys.py.

See [this guide](https://thalex.com/blog/how-to-run-a-thalex-bot-on-aws) 
about how you can get a bot up and running in the cloud.

Feel free to use it however you want, just remember that it comes without warranty.

If you spot any errors/bugs please report or create a pull request.
