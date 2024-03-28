import os

files = os.listdir('../../data/yse_dr1_zenodo/')

for file in files:
    with open('../../data/yse_dr1_zenodo/' + file, 'r') as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if lines[i].startswith('SPEC_CLASS:'):
                lines[i] = lines[i].replace('SN ', 'SN-')
    with open('../../data/yse_dr1_zenodo/' + file, 'w') as f:
        f.writelines(lines)
