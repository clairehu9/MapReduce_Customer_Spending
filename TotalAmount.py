# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 19:26:57 2019

@author: clair
"""

from mrjob.job import MRJob

class TotalAmount(MRJob):

    def mapper(self, _, line):
        (CustomID, ItemID, AmountSpent) = line.split(',')
        yield CustomID, float(AmountSpent) 
        
    def reducer(self, CustomID, AmountSpent):
        yield CustomID, sum(AmountSpent) 

if __name__ == '__main__':
    TotalAmount.run()
    
#!python TotalAmount.py DataA1.csv > TotalAmount.txt
