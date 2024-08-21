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

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Hr         | 16000 | 2  |
| Operations | 10000 | 3  |
| Developer  | 10000 | 4  |

```sql
Select *,row_number() over(partition by deoartment order by salary) as rn from company;
```

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Hr         | 16000 | 2  |
| Operations | 10000 | 1  |
| Developer  | 10000 | 1  |

## 2. Rank

Gives Rank to Each Row and will Skip ranks if encounters any same value

```sql
Select *,rank() over(order by salary) as rn from company;
```

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Operations | 10000 | 1  |
| Developer  | 10000 | 1  |
| Hr         | 16000 | 4  |

```sql
Select *,rank() over(partition by order by salary) as rn from company;
```

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Hr         | 16000 | 2  |
| Operations | 10000 | 1  |
| Developer  | 10000 | 1  |

## 3. Dense Rank

Gives Rank to each row based on condition by doesn`t skip any rank

```sql
Select *,dense_rank() over(order by salary) as rn from company;
```

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Operations | 10000 | 1  |
| Developer  | 10000 | 1  |
| Hr         | 16000 | 2  |

```sql
Select *,dense_rank() over(partition by order by salary) as rn from company;
```

| Department | Salary | rn |
| ---------- | :----: | -- |
| Hr         | 10000 | 1  |
| Hr         | 16000 | 2  |
| Operations | 10000 | 1  |
| Developer  | 10000 | 1  |


## 4. Lag

This window function is used to fetch the previous row records

```sql

Select *,lag(salary) over(order by salary) as prev from company;
```

| Department | Salary | prev |
| ---------- | :----: | -- |
| Hr         | 10000 | Null |
| Operations | 10000 | 10000 |
| Developer  | 10000 | 10000  |
| Hr         | 16000 | 10000  |

```sql
Select *,lag(salary) over(partition by department order by salary)as prev from company;
```


| Department | Salary | prev |
| ---------- | :----: | -- |
| Hr         | 10000 | Null  |
| Hr         | 16000 | 10000  |
| Operations | 10000 | Null  |
| Developer  | 10000 | Null  |

## 5. Lead

