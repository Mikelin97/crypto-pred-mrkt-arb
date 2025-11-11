import numpy as np

class Vol:
    def __init__(self, effective_memory):
        self.effective_memory = effective_memory
        self.var_lambda = 1-1/effective_memory
        self.secs_in_year = 3.154e+7
        self.vol = 0
        self.prev_price = np.nan
    
    def compute_vol(self, return_val):
        return return_val**2

    def update_vol_from_price(self, price):
        if np.isnan(self.prev_price) or price <= 0:
            return_val = 0.0
        else:
            return_val = np.log(price/self.prev_price)
        vol_measurement = self.compute_vol(return_val) * self.secs_in_year # make it annualized assuming secondly data TODO -> this may be bad if delta t isn't 1 second
        self.vol = np.sqrt((1-self.var_lambda) * vol_measurement + self.var_lambda * (self.vol)**2)
        self.prev_price = price