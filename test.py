# Ref. https://github.com/webdataset/webdataset-lightning/blob/main/train.py
import os
import random
import torch
from glob import glob
import webdataset as wds

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--dname', type=str)
parser.add_argument('--bucket', type=str, default='./shards')
args = parser.parse_args()


def decode(x):
    return x.decode()


def collate_fn(batch):
    print('hi')
    score, title = list(zip(*batch))
    return score, title

urls = sorted(glob(os.path.join(args.bucket, f'{args.dname}-*.tar')))
dataset = (
    wds.WebDataset(urls)
    .shuffle(0)
    .to_tuple("score", "title")
    .map_tuple(decode, decode)
)

loader = torch.utils.data.DataLoader(dataset, batch_size=3, collate_fn=collate_fn)
i = iter(loader)
print(next(i))
print(next(i))
