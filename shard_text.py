# Ref: https://github.com/webdataset/webdataset-lightning/blob/main/makeshards.py
import os
import random
import shutil
import webdataset as wds

import argparse
parser = argparse.ArgumentParser("Generate sharded dataset from text")
parser.add_argument('--source', type=str, help='source data path')
parser.add_argument('--dname', type=str, help='dataset name')
parser.add_argument('--header', type=str, help='column names of csv file', default='')
parser.add_argument('--bucket', type=str, help='directory for saving shards', default='./shards')
parser.add_argument('--maxsize', type=float, help='max file size per one shard', default=1e9)
parser.add_argument('--maxcount', type=int, help='max sample counts per one shard', default=1000)
parser.add_argument('--splits', type=str, help='train/valid/test split ratio (e.g. train:70,valid:20,test:10)', default='')
args = parser.parse_args()


def check_bucket(path, clear=True):
    if os.path.isdir(path):
        if clear:
            shutil.rmtree(path)
    
    if not os.path.isdir(path):
        os.makedirs(path)


def make_pattern(bucket, dname, split=None):
    if split:
        return os.path.join(bucket, f'{dname}-{split}-%08d.tar')
    else:
        return os.path.join(bucket, f'{dname}-%08d.tar')


def main(args):
    if args.splits:
        splits = [s.split(':') for s in args.splits.split(',')]
        split_names, split_ratios = list(zip(*splits))
        split_ratios = list(map(int, split_ratios))
        
        split_cumm_ratios = []
        curr = 0
        for r in split_ratios:
            curr += r
            split_cumm_ratios.append(curr)

        if split_cumm_ratios[-1] != 100:
            raise Exception('The sum of split ratio should be 100')

        writers = [wds.ShardWriter(make_pattern(args.bucket, args.dname, s), maxsize=args.maxsize, maxcount=args.maxcount) for s in split_names]
        print(split_cumm_ratios)

    else:
        writers = [wds.ShardWriter(make_pattern(args.bucket, args.dname), maxsize=args.maxsize, maxcount=args.maxcount)]

    
    ext = args.source.split('.')[-1]
    check_bucket(args.bucket)
    with open(args.source, 'r') as stream:
        for idx, record in enumerate(stream):
            sample_key = "%08d" % idx

            if ext == 'csv':
                # check header
                if idx == 0:
                    if args.header:
                        header = args.header.split(',')
                    else:
                        header = record.split(',')
                        continue
                
                record = record.split(',')
                sample = {h:r for h, r in zip(header, record)}
                sample['__key__'] = sample_key
                    

            elif ext == 'txt':
                sample = {"__key__": sample_key, "text": record}


            prob = random.random() * 100
            for widx, r in enumerate(split_cumm_ratios):
                if prob < r:
                    break
            writers[widx].write(sample)    

    for w in writers:
        w.close()

if __name__ == '__main__':
    main(args)