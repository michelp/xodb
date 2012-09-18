from __future__ import absolute_import

from cStringIO import StringIO

from xapian import SimpleStopper
from . import english, spanish, french, german, italian, russian

def build_stopwords(language, encoding="utf8"):
    file = StringIO(language.stopwords)
    stopwords = []
    for line in file.readlines():
        word = unicode(line.strip().split("|")[0].strip(), encoding)
        if word:
            stopwords.append(word)
    return stopwords

stopwords = {
    "en" : build_stopwords(english, encoding="latin-1"),
    "es" : build_stopwords(spanish, encoding="latin-1"),
    "ru" : build_stopwords(russian, encoding="koi8_r"),
    "fr" : build_stopwords(french, encoding="latin-1"),
    "de" : build_stopwords(german, encoding="latin-1"),
    "it" : build_stopwords(italian, encoding="latin-1"),
}

stoppers = {}

for code in stopwords:
    stopper = SimpleStopper()
    for word in stopwords[code]:
        stopper.add(word)
    stoppers[code] = stopper





