
file = open("winequality-red.csv", "r")
l = []

text = file.read()
file.close()
i = 0
while (i<15000):
    l.append(text)
    i += 1

final_text = "".join(l)

writer = open("data.txt", "w")
writer.write(final_text)
writer.close()