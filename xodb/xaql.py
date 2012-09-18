import pyparsing

"""
The query language that won't die.

Syntax:

    Typical search engine query language, terms with boolean operators
    and parenthesized grouping:

      (term AND (term OR term OR ...) AND NOT term ...)

    In it's simplest case, xaql searches for a list of terms:

      term term term ...

    This expands to '(term AND term AND term AND ...)'

    Terms can be prefixed.  The prefix and the value are separated by
    colons.  Values that contain spaces must be double quoted.

      term color:blue name:"My Lagoon"


Functions:

    Functions provide features that take query input from the user and
    do some transformation on the query itself.  Functions begin with
    a star, then a name, then a pair of parenthesis that contain the
    query input.  The syntax of the input is up to the function:

    $xp(...) -- Pass the input string directly into the Xapian
     query parser.

    $rel(...)

    $now


Prefix Modifiers:

  This are pre-prefixes that transform the following term.

    published-before:now
    

"""

