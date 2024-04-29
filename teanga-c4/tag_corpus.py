import gzip
import requests 
import spacy
import sys
import json
from io import BytesIO
import tqdm

def main(n : int):
    nlp = spacy.load('en_core_web_sm')
    for i in range(n):
        print("Downloading segment %d" % i)
        url = "https://huggingface.co/datasets/allenai/c4/resolve/main/en/c4-train.%05d-of-01024.json.gz?download=true" % i
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            buffer = BytesIO(response.content)

            with gzip.open("c4-train.%05d-of-01024.json.gz" % i, 'wt') as out:
                with gzip.GzipFile(fileobj=buffer, mode="r") as f:
                    for line in tqdm.tqdm(f, total=356000):
                        data = json.loads(line)
                        doc = nlp(data['text'])
                        data["words"] = [[token.idx, token.idx + len(token.text)] for token in doc]
                        data["pos"] = [token.pos_ for token in doc]
                        data["lemma"] = [token.lemma_ for token in doc]
                        out.write(json.dumps(data))
                        out.write("\n")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python tag_corpus.py max_segs')
        sys.exit(1)
    main(int(sys.argv[1]))



