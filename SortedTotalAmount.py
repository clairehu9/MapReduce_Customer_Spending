# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 19:35:22 2019

@author: clair
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class SortedTotalAmount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_customer_data,
                   reducer=self.reducer_total_spending),
            MRStep(mapper=self.mapper_lead_zeros,
                   reducer = self.reducer_output)
        ]
        
    def mapper_customer_data(self, _, line):
        (CustomID, ItemID, AmountSpent) = line.split(',')
        yield CustomID, float(AmountSpent) #find out each customer each purchase
        
    def reducer_total_spending(self, CustomID, AmountSpent):
        yield CustomID, sum(AmountSpent) #find out each customer total purchase not sorted

    def mapper_lead_zeros(self, CustomerID, AmountSpent): #want to sort
        yield '%04.02f'%float(AmountSpent), CustomerID

    def reducer_output(self, AmountSpent, CustomerIDs):
        for CustomerID in CustomerIDs:
            yield AmountSpent, CustomerID

if __name__ == '__main__':
    SortedTotalAmount.run()

# !python SortedTotalAmount.py DataA1.csv > SortedTotalAmount.txt