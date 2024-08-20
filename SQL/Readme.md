# Window Functions

1. Row Number
2. Rank
3. Dense Rank
4. Lag
5. Lead

## 1. Row Number

Gives  Number to each row, no number will repeat regardless of same or different values

Suppose we have one Company table :- 

| Department | Salary |
| ---------- | :----: |
| Hr         | 10000 |
| Hr         | 16000 |
| Operations | 10000 |
| Developer  | 10000 |

```sql
Select *,row_number() over(order by salary) as rn from company;
```
