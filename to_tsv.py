import sys
import pandas

t = pandas.read_csv(sys.argv[1])
f = t.to_csv(sys.argv[1].replace('.csv', '.tsv'), sep="\t", index=False, header=False)
