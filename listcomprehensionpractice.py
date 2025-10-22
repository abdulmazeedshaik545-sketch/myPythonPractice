Python 3.13.7 (v3.13.7:bcee1c32211, Aug 14 2025, 19:10:51) [Clang 16.0.0 (clang-1600.0.26.6)] on darwin
Enter "help" below or click "Help" above for more information.
numbers = [1,2,3,4,5]
squares = [x*x for x in numbers]
squares
[1, 4, 9, 16, 25]
#From a list of words, make a new list with all words in uppercase.
words = ["apple", "banana", "cherry"]
upperwords = [x.upper() for x in words]
upperwords
['APPLE', 'BANANA', 'CHERRY']
#Extract all even numbers from a given list.
nums = [10, 21, 4, 45, 66, 93]
evens = [x for x in nums if x%2 == 0]
evens
[10, 4, 66]
#From a list of fruits, select only those containing the letter 'a'.
fruits = ["apple", "banana", "cherry", "papaya", "watermelom", "litchi"]
afruits = [x for x in fruits if 'a' in x]
afruits
['apple', 'banana', 'papaya', 'watermelom']
#Create a new list of first letters of each fruit.
newlistoffruits = [fruits[0] for x in fruits]
newlistoffruits
['apple', 'apple', 'apple', 'apple', 'apple', 'apple']
newlist = [x[0] for x in fruits]
newlist
['a', 'b', 'c', 'p', 'w', 'l']
#Convert a list of Celsius temperatures to Fahrenheit.
#Formula: F = (C * 9/5) + 32
F = [(C * 9/5) + 32 for c in celsius]
Traceback (most recent call last):
  File "<pyshell#21>", line 1, in <module>
    F = [(C * 9/5) + 32 for c in celsius]
NameError: name 'celsius' is not defined
celsius = [30,35,40,45,50]
F = [(C * 9/5) + 32 for c in celsius]
Traceback (most recent call last):
  File "<pyshell#23>", line 1, in <module>
    F = [(C * 9/5) + 32 for c in celsius]
NameError: name 'C' is not defined
F = [(c * 9/5) + 32 for c in celsius]
F
[86.0, 95.0, 104.0, 113.0, 122.0]
#Given a list of strings, create a new list containing their lengths.
words = ["hi", "hello", "python"]
lenofwords = [len(x) for x in words]
lenofwords
[2, 5, 6]
#Replace all negative numbers in a list with 0.
nums = [3, -1, 5, -9, 2]
removingnegative = [x for x in numbers if x < 0]
removingnegative
[]
removingnegative = [x.append(x) for x in numbers if x < 0]
removingnegative
[]
removingnegatiev = [n if n>0 else o for n in numbers]
removingnegatiev
[1, 2, 3, 4, 5]
removingnegatiev = [n if n>0 else o for n in nums]
Traceback (most recent call last):
  File "<pyshell#38>", line 1, in <module>
    removingnegatiev = [n if n>0 else o for n in nums]
NameError: name 'o' is not defined
removingnegative =  [n if n > 0 else 0 for n in nums]
removingnegative
[3, 0, 5, 0, 2]
#Create a list of tuples containing numbers and their squares.
nums = [1, 2, 3]
numtuple = [(n, n**2) for n in nums ]
numtuple
[(1, 1), (2, 4), (3, 9)]
#Extract all vowels from a given string using a comprehension.
text = "programming"
vowels = [ch for ch in text if ch in 'aeiou']
vowels
['o', 'a', 'i']
#Filter words longer than 4 letters from a list.
newwords = ["encapsulation", "inheritance", "objecthandling", "class", "structure"]
filterwords = [len(x) > 4 for x in newords]
Traceback (most recent call last):
  File "<pyshell#51>", line 1, in <module>
    filterwords = [len(x) > 4 for x in newords]
NameError: name 'newords' is not defined. Did you mean: 'newwords'?
filterwords = [len(x) > 4 for x in newwords]
filterwords
[True, True, True, True, True]
filterwords = [w for w in words if len(w) > 4]
filterworkds
Traceback (most recent call last):
  File "<pyshell#55>", line 1, in <module>
    filterworkds
NameError: name 'filterworkds' is not defined. Did you mean: 'filterwords'?
filterwords
['hello', 'python']
filterwords = [w for w in newwords if len(w) > 4]
filterwords
['encapsulation', 'inheritance', 'objecthandling', 'class', 'structure']
#From a list of words, create a new list with words that start with ‘b’.
words = ["basketball", "baseball", "apple", "baloon"]
startswithb =  [w for w in words if w[0] == 'b']
startswithb
['basketball', 'baseball', 'baloon']
startswithb = [w for w in words if w.startswith('b')]
startswithb
['basketball', 'baseball', 'baloon']
#Given two lists, create a list of pairs (a, b) only when a != b.
a = [1,2,3]
b = [3,1,4]
newlist = [(x, y) for x in a for y in b if x != y]
newlist
[(1, 3), (1, 4), (2, 3), (2, 1), (2, 4), (3, 1), (3, 4)]
#Transpose a matrix using a nested comprehension.
matrix = [[1, 2, 3], [4, 5, 6]]
matixtranpose = [[row[i] for row in matrix] for i in range(len(matrix[0]))]
matixtranpose
[[1, 4], [2, 5], [3, 6]]
#Create a list of numbers from 1–50 that are divisible by both 3 and 5.
numers = [x for x in range(50) if x % 3 == 0 and X % 5 == 0]
Traceback (most recent call last):
  File "<pyshell#75>", line 1, in <module>
    numers = [x for x in range(50) if x % 3 == 0 and X % 5 == 0]
NameError: name 'X' is not defined
numers = [x for x in range(50) if x % 3 == 0 and x % 5 == 0]
numers
[0, 15, 30, 45]
>>> numers = [x for x in range(1, 50) if x % 3 == 0 and x % 5 == 0]
>>> numers
[15, 30, 45]
>>> numers = [x for x in range(1, 51) if x % 3 == 0 and x % 5 == 0]
>>> numers
[15, 30, 45]
>>> #Given a dictionary, create a list of keys whose values are greater than 10.
>>> data = {'a':5, 'b':15, 'c':8, 'd':20}
>>> pairkeys = [k for (k, v) in data]
Traceback (most recent call last):
  File "<pyshell#84>", line 1, in <module>
    pairkeys = [k for (k, v) in data]
ValueError: not enough values to unpack (expected 2, got 1)
>>> pairkeys = [k for (k, v) in data.enumarated()]
Traceback (most recent call last):
  File "<pyshell#85>", line 1, in <module>
    pairkeys = [k for (k, v) in data.enumarated()]
AttributeError: 'dict' object has no attribute 'enumarated'
>>> pairkeys = [k for (k, v) in enumare(data)]
Traceback (most recent call last):
  File "<pyshell#86>", line 1, in <module>
    pairkeys = [k for (k, v) in enumare(data)]
NameError: name 'enumare' is not defined. Did you mean: 'enumerate'?
>>> pairkeys = [k for (k, v) in enumerate(data)]
>>> pairkeys
[0, 1, 2, 3]
>>> pairkeys = [k for k, v in data.items if v > 10]
Traceback (most recent call last):
  File "<pyshell#89>", line 1, in <module>
    pairkeys = [k for k, v in data.items if v > 10]
TypeError: 'builtin_function_or_method' object is not iterable
>>> pairkeys = [k for k, v in data.items() if v > 10]
>>> pairkeys
['b', 'd']
>>> listofStrings = ["madam", "apple", "racecar", "level", "banana"]
... 
>>> palindromes = [s for s in listofStrings if s == s[::-1]]
>>> palindromes
['madam', 'racecar', 'level']
