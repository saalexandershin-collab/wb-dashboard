import sys, os
sys.path.insert(0, ".")
from src.db.models import init_db, get_session_factory
from src.db.repository import OzonTransactionRepository, OzonPostingRepository

DB_URL = os.environ["DATABASE_URL"]
engine = init_db(DB_URL)
Session = get_session_factory(engine)

with Session() as s:
    df_tx   = OzonTransactionRepository().get_by_month(s, 2026, 4)
    df_post = OzonPostingRepository().get_by_month(s, 2026, 4)

print(f"Транзакций: {len(df_tx)}, Постингов: {len(df_post)}")

# Выплата = все транзакции
oz_payout = df_tx["amount"].fillna(0).sum()

# Продажи
sales_mask   = df_tx["operation_type"] == "OperationAgentDeliveredToCustomer"
returns_mask = df_tx["operation_type"] == "OperationReturnGoodsFulfillmentByMarketplace"
oz_sales_n   = sales_mask.sum()
oz_returns_n = returns_mask.sum()
oz_accruals  = df_tx.loc[sales_mask, "accruals_for_sale"].fillna(0).sum()
oz_commission= df_tx.loc[sales_mask, "sale_commission"].fillna(0).sum()

# Оборот из постингов (доставленные × цена)
if not df_post.empty:
    delivered   = df_post[~df_post["is_cancelled"]]
    oz_turnover = (delivered["price"] * delivered["quantity"]).sum()
    oz_qty      = int(delivered["quantity"].sum())
else:
    oz_turnover = oz_qty = 0

# НДС 5% (внутри)
nds = oz_turnover * 5 / 105
# УСН 6% от выплаты минус НДС-часть
usn_base = oz_payout - oz_payout * 5 / 105
usn      = usn_base * 6 / 100
total_tax = nds + usn
net       = oz_payout - total_tax

print(f"OZON_TURNOVER={oz_turnover:.2f}")
print(f"OZON_ACCRUALS={oz_accruals:.2f}")
print(f"OZON_COMMISSION={oz_commission:.2f}")
print(f"OZON_PAYOUT={oz_payout:.2f}")
print(f"OZON_SALES_N={oz_sales_n}")
print(f"OZON_RETURNS_N={oz_returns_n}")
print(f"OZON_QTY={oz_qty}")
print(f"NDS={nds:.2f}")
print(f"USN_BASE={usn_base:.2f}")
print(f"USN={usn:.2f}")
print(f"TOTAL_TAX={total_tax:.2f}")
print(f"NET={net:.2f}")
