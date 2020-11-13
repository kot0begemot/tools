import numpy as np
from scipy.special import betaln #ln(abs(beta(x)))
a_A, b_A, a_B, b_B = converted_A + 1, total_A - converted_A, converted_B + 1, total_B - converted_B
result = 0
for i in range(a_B):
    Prob_pB_vs_pA = np.exp(betaln(a_A+i, b_B+b_A) - np.log(b_B+i) - betaln(1+i, b_B) - betaln(a_A, b_A))
    result += Prob_pB_vs_pA
return result
