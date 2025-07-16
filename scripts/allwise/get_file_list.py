import os

os.system(
    "wget https://irsa.ipac.caltech.edu/data/download/parquet/wise/allwise/healpix_k5/wise-allwise-download-irsa-wget.sh -O _file_list.txt"
)

out_lines = []

with open("_file_list.txt", "r") as f:
    lines = f.readlines()
    for line in lines:
        if "wise-allwise.parquet" in line:
            out_lines.append("https" + line.split("https")[1].strip())

with open("file_list.txt", "w") as f:
    f.write("\n".join(out_lines))
