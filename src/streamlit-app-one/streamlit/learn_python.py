import pandas as pd

df = pd.read_csv("data/employees.csv", header=0).convert_dtypes()

edges = ""
for _, ass in df.iterrows():
    if not pd.isna(ass.iloc[1]):
        edges += f'\t"{ass.iloc[0]}" -> "{ass.iloc[1]}";\n'

d = f'digraph {{\n{edges}}}'
print(d)