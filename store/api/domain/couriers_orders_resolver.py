import asyncio


class CouriersOrdersResolver:
    def __init__(self, orders_, max_weight, values_=None):
        self.orders = {}
        i = 1
        for k, v in orders_.items():
            # as weight is a float number, convert to int by multiplying by 100 and rounding
            # (minimal value is 0.01, so it'll be converted to 1)
            self.orders[i] = {'id': k, 'weight': int(v * 100)}
            i += 1
        self.w = int(max_weight * 100)
        self.n = len(orders_)
        self.val = [1 for i in range(len(orders_))] if values_ is None else values_
        self.k = [[0 for x in range(self.w + 1)] for x in range(self.n + 1)]
        self.ans = []

    async def resolve_orders(self):
        await self.solve_knapsack_problem()
        await self.find_ans(self.n, self.w)
        ids_ = []
        for item in self.ans:
            ids_.append(self.orders[item]['id'])
        ids_.sort()
        return ids_

    async def solve_knapsack_problem(self):
        # Build table k[][] in bottom up manner
        for i in range(self.n + 1):
            for w in range(self.w + 1):
                if i == 0 or w == 0:
                    self.k[i][w] = 0
                elif self.orders[i]['weight'] <= w:
                    self.k[i][w] = \
                        max(self.val[i - 1] + self.k[i - 1][w - self.orders[i]['weight']], self.k[i - 1][w])
                else:
                    self.k[i][w] = self.k[i - 1][w]
        return self.k[self.n][self.w]

    async def find_ans(self, k, s):
        if self.k[k][s] == 0:
            return
        if self.k[k - 1][s] == self.k[k][s]:
            await self.find_ans(k - 1, s)
        else:
            await self.find_ans(k - 1, s - self.orders[k]['weight'])
            self.ans.append(k)


if __name__ == "__main__":
    # test case for algorithm
    orders = {0: 3.1, 1: 4.1, 2: 5.1, 3: 8.1, 4: 9.1}
    values = [1, 6, 4, 7, 6]
    max_w = 13.5
    resolver = CouriersOrdersResolver(orders_=orders, values_=values, max_weight=max_w)
    loop = asyncio.get_event_loop()
    ids = loop.run_until_complete(resolver.resolve_orders())
    loop.close()
    print(ids)
