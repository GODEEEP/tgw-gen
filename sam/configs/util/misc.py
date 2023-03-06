# -*- coding: utf-8 -*-
"""
Created on Feb 10 08:46:43 2023

@author: Cameron Bracken (cameron.bracken@pnnl.gov)
"""


def dedup_names(names):
  # stolen from an old version of pandas
  # https://stackoverflow.com/questions/24685012/
  # pandas-dataframe-renaming-multiple-identically-named-columns

  names = list(names)  # so we can index
  counts = {}

  for i, col in enumerate(names):
    cur_count = counts.get(col, 0)

    if cur_count > 0:
      names[i] = '%s_%d' % (col, cur_count)

    counts[col] = cur_count + 1

  return names
