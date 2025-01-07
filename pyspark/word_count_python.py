from collections import Counter

filename = "/Users/ganeshmoorthy/Desktop/coding/medium/input/word_count.txt"
a = list()
with open(filename, 'r') as fh:
    lines = fh.read()

print(lines)
line = lines.replace("\n", " ")
print(line)
words = line.split(" ")
a.extend(words)

word_count = Counter(a)
print(word_count)
for word, count in word_count.items():
    print(f"{word}: {count}")
