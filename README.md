# Intro

This is a free to use python exchange connector library for [Thalex](https://www.thalex.com).

It is provided free of charge, as is, with no warranties and no recommendations – see MIT license.
It is not meant to be an advisory tool, nor an inducement, but to lower the development learning curve for users.

See also the [api documentation](https://www.thalex.com/docs/).

thalex.py has a function for every websocket endpoint with typehints and a recieve function 
that returns the messages from the exchange one by one.

# How to install

The easiest is just
```
pip install thalex
```

Alternatively, if you want the examples as well, you can
```
git clone https://github.com/thalextech/thalex_py.git
cd thalex_py
pip install -e ./
```

# Examples

There are some examples on how you could use this library in the examples folder.

Keep in mind that the examples are not meant to be financial advice,
they just illustrate what an implementation of a trading bot could look like.

If you want to run the examples, you have to rename/copy _keys.py to keys.py, 
create api keys on thalex ui and put them in keys.py.

# How to run bots 24/7 in the cloud

See [this guide](https://thalex.com/blog/how-to-run-a-thalex-bot-on-aws) 
about how you can get a bot up and running in the cloud.

# NFA, your warranty is void

Feel free to use this library however you want, just remember that nothing is financial advice here and
nothing comes with warranty.

# Issues

If you spot any errors/bugs please report or create a pull request.

You can also reach out on thalex_py@thalex.com
