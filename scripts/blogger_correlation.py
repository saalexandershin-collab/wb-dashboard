"""
Blogger-Sales Correlation Analysis
Queries production DB for weekly WB sales, correlates with blogger spend.
Outputs a full text report to stdout.
"""
import os, json, sys, math
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# ── Blogger weekly spend data (extracted from Excel files) ────────────────────
# Sources:
#   /Downloads/Сыворотка СН6.xlsx            → Sheet "Сыворотка"         → product=CH6
#   /Downloads/Работа с блогерам инфлюенс.xlsx → Sheets: Март Наталья,
#                                                 Апрель Наталья,
#                                                 Милана апрель,
#                                                 Милана май
# All costs in RUB. Barter (бартер) = 0 ad spend.
# week_start = Monday of ISO week.
WEEKLY_BLOGGER_SPEND = {
    # CH6 Сыворотка — before Nov 2025 (included for reference, not in correlation window)
    "CH6_2025-07-07": {"product":"CH6","week_start":"2025-07-07","ad_spend":235000,"product_cost":13501,"total_spend":248501,"n_bloggers":8},
    "CH6_2025-07-14": {"product":"CH6","week_start":"2025-07-14","ad_spend":155000,"product_cost":22876,"total_spend":177876,"n_bloggers":12},
    "CH6_2025-07-21": {"product":"CH6","week_start":"2025-07-21","ad_spend":152200,"product_cost":19540,"total_spend":171740,"n_bloggers":9},
    "CH6_2025-07-28": {"product":"CH6","week_start":"2025-07-28","ad_spend":221500,"product_cost":17895,"total_spend":239395,"n_bloggers":11},
    "CH6_2025-08-04": {"product":"CH6","week_start":"2025-08-04","ad_spend":174000,"product_cost":13268,"total_spend":187268,"n_bloggers":9},
    "CH6_2025-08-11": {"product":"CH6","week_start":"2025-08-11","ad_spend":160000,"product_cost":28803,"total_spend":188803,"n_bloggers":13},
    "CH6_2025-08-18": {"product":"CH6","week_start":"2025-08-18","ad_spend":150970,"product_cost":22783,"total_spend":173753,"n_bloggers":11},
    "CH6_2025-08-25": {"product":"CH6","week_start":"2025-08-25","ad_spend":13000,"product_cost":5238,"total_spend":18238,"n_bloggers":2},
    "CH6_2025-09-01": {"product":"CH6","week_start":"2025-09-01","ad_spend":68000,"product_cost":11917,"total_spend":79917,"n_bloggers":7},
    "CH6_2025-09-08": {"product":"CH6","week_start":"2025-09-08","ad_spend":110500,"product_cost":14490,"total_spend":124990,"n_bloggers":7},
    "CH6_2025-09-15": {"product":"CH6","week_start":"2025-09-15","ad_spend":115000,"product_cost":17678,"total_spend":132678,"n_bloggers":8},
    "CH6_2025-09-22": {"product":"CH6","week_start":"2025-09-22","ad_spend":45500,"product_cost":5015,"total_spend":50515,"n_bloggers":5},
    "CH6_2025-09-29": {"product":"CH6","week_start":"2025-09-29","ad_spend":37960,"product_cost":7237,"total_spend":45197,"n_bloggers":3},
    "CH6_2025-10-13": {"product":"CH6","week_start":"2025-10-13","ad_spend":17360,"product_cost":2885,"total_spend":20245,"n_bloggers":3},
    "CH6_2025-10-20": {"product":"CH6","week_start":"2025-10-20","ad_spend":56000,"product_cost":13558,"total_spend":69558,"n_bloggers":7},
    "CH6_2025-10-27": {"product":"CH6","week_start":"2025-10-27","ad_spend":69500,"product_cost":7230,"total_spend":76730,"n_bloggers":5},
    # CH6 Сыворотка — Nov 2025 onward (correlation window)
    "CH6_2025-11-03": {"product":"CH6","week_start":"2025-11-03","ad_spend":65000,"product_cost":11178,"total_spend":76178,"n_bloggers":5},
    "CH6_2025-12-01": {"product":"CH6","week_start":"2025-12-01","ad_spend":52500,"product_cost":5563,"total_spend":58063,"n_bloggers":5},
    "CH6_2025-12-08": {"product":"CH6","week_start":"2025-12-08","ad_spend":110000,"product_cost":6520,"total_spend":116520,"n_bloggers":6},
    "CH6_2025-12-15": {"product":"CH6","week_start":"2025-12-15","ad_spend":139000,"product_cost":6180,"total_spend":145180,"n_bloggers":6},
    "CH6_2025-12-22": {"product":"CH6","week_start":"2025-12-22","ad_spend":60000,"product_cost":8342,"total_spend":68342,"n_bloggers":5},
    "CH6_2026-01-05": {"product":"CH6","week_start":"2026-01-05","ad_spend":71100,"product_cost":4086,"total_spend":75186,"n_bloggers":3},
    "CH6_2026-01-12": {"product":"CH6","week_start":"2026-01-12","ad_spend":99000,"product_cost":14457,"total_spend":113457,"n_bloggers":9},
    "CH6_2026-01-19": {"product":"CH6","week_start":"2026-01-19","ad_spend":129400,"product_cost":17006,"total_spend":146406,"n_bloggers":10},
    "CH6_2026-01-26": {"product":"CH6","week_start":"2026-01-26","ad_spend":110900,"product_cost":18299,"total_spend":129199,"n_bloggers":10},
    "CH6_2026-02-02": {"product":"CH6","week_start":"2026-02-02","ad_spend":71000,"product_cost":1972,"total_spend":72972,"n_bloggers":3},
    "CH6_2026-02-09": {"product":"CH6","week_start":"2026-02-09","ad_spend":15000,"product_cost":2065,"total_spend":17065,"n_bloggers":1},
    "CH6_2026-02-16": {"product":"CH6","week_start":"2026-02-16","ad_spend":89500,"product_cost":14593,"total_spend":104093,"n_bloggers":9},
    "CH6_2026-02-23": {"product":"CH6","week_start":"2026-02-23","ad_spend":144180,"product_cost":14730,"total_spend":158910,"n_bloggers":9},
    "CH6_2026-03-02": {"product":"CH6","week_start":"2026-03-02","ad_spend":95000,"product_cost":10556,"total_spend":105556,"n_bloggers":6},
    "CH6_2026-03-09": {"product":"CH6","week_start":"2026-03-09","ad_spend":17000,"product_cost":2055,"total_spend":19055,"n_bloggers":1},
    "CH6_2026-03-16": {"product":"CH6","week_start":"2026-03-16","ad_spend":34000,"product_cost":5906,"total_spend":39906,"n_bloggers":3},
    "CH6_2026-03-23": {"product":"CH6","week_start":"2026-03-23","ad_spend":92000,"product_cost":9661,"total_spend":101661,"n_bloggers":7},
    "CH6_2026-03-30": {"product":"CH6","week_start":"2026-03-30","ad_spend":82500,"product_cost":9999,"total_spend":92499,"n_bloggers":6},
    "CH6_2026-04-06": {"product":"CH6","week_start":"2026-04-06","ad_spend":88000,"product_cost":11972,"total_spend":99972,"n_bloggers":6},
    "CH6_2026-04-13": {"product":"CH6","week_start":"2026-04-13","ad_spend":107000,"product_cost":12637,"total_spend":119637,"n_bloggers":6},
    "CH6_2026-04-20": {"product":"CH6","week_start":"2026-04-20","ad_spend":115000,"product_cost":12426,"total_spend":127426,"n_bloggers":7},
    "CH6_2026-04-27": {"product":"CH6","week_start":"2026-04-27","ad_spend":185000,"product_cost":27436,"total_spend":212436,"n_bloggers":14},
    "CH6_2026-05-04": {"product":"CH6","week_start":"2026-05-04","ad_spend":105000,"product_cost":22591,"total_spend":127591,"n_bloggers":11},
    "CH6_2026-05-11": {"product":"CH6","week_start":"2026-05-11","ad_spend":45500,"product_cost":10231,"total_spend":55731,"n_bloggers":5},
    "CH6_2026-05-18": {"product":"CH6","week_start":"2026-05-18","ad_spend":122000,"product_cost":21020,"total_spend":143020,"n_bloggers":10},
    "CH6_2026-05-25": {"product":"CH6","week_start":"2026-05-25","ad_spend":124502,"product_cost":23764,"total_spend":148266,"n_bloggers":11},
    "CH6_2026-06-01": {"product":"CH6","week_start":"2026-06-01","ad_spend":40900,"product_cost":3981,"total_spend":44881,"n_bloggers":3},
    "CH6_2026-06-15": {"product":"CH6","week_start":"2026-06-15","ad_spend":56000,"product_cost":5992,"total_spend":61992,"n_bloggers":3},
    # SPF Curacion — Apr-May 2026
    "SPF_2026-04-06": {"product":"SPF","week_start":"2026-04-06","ad_spend":35000,"product_cost":1620,"total_spend":36620,"n_bloggers":1},
    "SPF_2026-04-13": {"product":"SPF","week_start":"2026-04-13","ad_spend":12000,"product_cost":6432,"total_spend":18432,"n_bloggers":4},
    "SPF_2026-04-20": {"product":"SPF","week_start":"2026-04-20","ad_spend":128120,"product_cost":20720,"total_spend":148840,"n_bloggers":13},
    "SPF_2026-04-27": {"product":"SPF","week_start":"2026-04-27","ad_spend":256040,"product_cost":19247,"total_spend":275287,"n_bloggers":12},
    "SPF_2026-05-04": {"product":"SPF","week_start":"2026-05-04","ad_spend":45110,"product_cost":6879,"total_spend":51989,"n_bloggers":3},
    "SPF_2026-05-11": {"product":"SPF","week_start":"2026-05-11","ad_spend":21000,"product_cost":3184,"total_spend":24184,"n_bloggers":2},
}

# Key bloggers for each product (for the findings section)
TOP_BLOGGERS = {
    "CH6": [
        ("2025-07-07", "@namennaa", 135000, 85000, "biggest single spend in Jul launch"),
        ("2025-07-28", "@multiple×11", None, 221500, "peak Jul spend week"),
        ("2025-12-15", "@multiple×6", None, 139000, "peak Dec spend (@ninosha__, @igizovaindira, @shshkat)"),
        ("2026-02-23", "@multiple×9", None, 144180, "peak Feb spend (@_balganna_, @klemanya, @alsuprobeauty...)"),
        ("2026-04-27", "@multiple×14", None, 185000, "peak Apr spend - 14 bloggers incl @annabateho @divegandi"),
    ],
    "SPF": [
        ("2026-04-11", "@lisa__es", 101000, 35000, "first SPF blogger (101k followers)"),
        ("2026-04-27", "@divegandi", 130000, 40000, "largest SPF blogger by spend"),
        ("2026-04-27", "@boga.eva_+@beautypolya+@lavrison", None, 275287, "peak SPF week - 12 bloggers"),
        ("2026-04-21", "@dashazayans", 75600, 21000, "Milana's SPF push"),
    ],
}


# ── Step 1: Get WB weekly sales from DB ──────────────────────────────────────
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT supplier_article, subject, nm_id, COUNT(*) as cnt
        FROM wb_orders
        WHERE (is_cancel = false OR is_cancel IS NULL)
        GROUP BY supplier_article, subject, nm_id
        ORDER BY cnt DESC
        LIMIT 100
    """))
    products_db = [dict(r._mapping) for r in result]

    print("=== PRODUCTS IN DB ===", file=sys.stderr)
    for p in products_db:
        print(f"  nm_id={p['nm_id']}  cnt={p['cnt']}  article={p['supplier_article']}  subject={p['subject']}", file=sys.stderr)

    result2 = conn.execute(text("""
        SELECT
            DATE_TRUNC('week', order_date::timestamp) as week_start,
            supplier_article,
            subject,
            nm_id,
            COUNT(*) as orders
        FROM wb_orders
        WHERE (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= '2025-11-01'
        GROUP BY week_start, supplier_article, subject, nm_id
        ORDER BY week_start, orders DESC
    """))
    weekly_raw = [dict(r._mapping) for r in result2]
    for row in weekly_raw:
        row['week_start'] = str(row['week_start'])[:10]


# ── Step 2: Classify DB products ─────────────────────────────────────────────
def classify_product(article, subject):
    a = str(article).lower()
    s = str(subject).lower()
    if any(x in a or x in s for x in ['спф', 'spf', 'curacion']):
        return "SPF"
    if any(x in a or x in s for x in ['сыворотка', 'serum', 'ch6', 'сн6']):
        return "CH6"
    return "Other"

df_sales_raw = pd.DataFrame(weekly_raw) if weekly_raw else pd.DataFrame(
    columns=["week_start", "supplier_article", "subject", "nm_id", "orders"]
)

if len(df_sales_raw) > 0:
    df_sales_raw["product"] = df_sales_raw.apply(
        lambda r: classify_product(r["supplier_article"], r["subject"]), axis=1
    )
    weekly_sales = (
        df_sales_raw
        .groupby(["week_start", "product"])
        .agg(orders=("orders", "sum"))
        .reset_index()
        .sort_values(["product", "week_start"])
    )
else:
    weekly_sales = pd.DataFrame(columns=["week_start", "product", "orders"])


# ── Step 3: Build blogger weekly DataFrames ───────────────────────────────────
blogger_rows = list(WEEKLY_BLOGGER_SPEND.values())
df_blogger = pd.DataFrame(blogger_rows)
df_blogger["week_start"] = pd.to_datetime(df_blogger["week_start"])
df_blogger = df_blogger.sort_values(["product", "week_start"])


# ── Step 4: Pearson correlation ───────────────────────────────────────────────
def pearson_r(x, y):
    n = len(x)
    if n < 3:
        return float('nan')
    mx, my = sum(x) / n, sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    dx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    dy = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if dx == 0 or dy == 0:
        return float('nan')
    return num / (dx * dy)


def interpret_r(r):
    if math.isnan(r):
        return f"n/a (insufficient data)"
    a = abs(r)
    sign = "positive" if r > 0 else "negative"
    if a >= 0.7:
        strength = "STRONG"
    elif a >= 0.4:
        strength = "moderate"
    elif a >= 0.2:
        strength = "weak"
    else:
        strength = "negligible"
    return f"{r:+.3f} ({strength} {sign})"


def build_corr_table(product_code, lag_weeks=1, start_date="2025-11-01"):
    b = df_blogger[df_blogger["product"] == product_code][
        ["week_start", "total_spend", "n_bloggers"]
    ].copy()
    if len(weekly_sales) == 0:
        return None, float('nan'), float('nan')

    s = weekly_sales[weekly_sales["product"] == product_code][
        ["week_start", "orders"]
    ].copy()
    s["week_start"] = pd.to_datetime(s["week_start"])

    # Filter to correlation window
    b = b[b["week_start"] >= pd.Timestamp(start_date)].copy()

    if len(b) == 0 or len(s) == 0:
        return None, float('nan'), float('nan')

    b = b.set_index("week_start")
    s = s.set_index("week_start")

    all_weeks = sorted(set(b.index) | set(s.index))
    b = b.reindex(all_weeks, fill_value=0)
    s = s.reindex(all_weeks, fill_value=0)

    table = pd.DataFrame({
        "week_start": all_weeks,
        "blogger_spend": b["total_spend"].values,
        "n_bloggers": b["n_bloggers"].values,
        "orders_same_week": s["orders"].values,
    })
    table["orders_next_week"] = table["orders_same_week"].shift(-lag_weeks)

    r_same = pearson_r(
        table["blogger_spend"].tolist(),
        table["orders_same_week"].tolist()
    )
    lag_df = table.dropna(subset=["orders_next_week"])
    r_lag = pearson_r(
        lag_df["blogger_spend"].tolist(),
        lag_df["orders_next_week"].tolist()
    )
    return table, r_same, r_lag


# ── Step 5: Print full report ─────────────────────────────────────────────────
SEP = "=" * 80
SEP2 = "-" * 70

print(SEP)
print("  BLOGGER-SALES CORRELATION ANALYSIS — WILDBERRIES")
print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
print(SEP)

# ── 1. Products in DB ─────────────────────────────────────────────────────────
print("\n1. PRODUCTS IN DATABASE")
print(SEP2)
if products_db:
    print(f"  {'nm_id':>12}  {'Orders':>7}  {'Article':<25}  Subject")
    print(f"  {'-'*12}  {'-'*7}  {'-'*25}  {'-'*30}")
    for p in products_db:
        label = classify_product(p['supplier_article'], p['subject'])
        print(f"  {str(p['nm_id']):>12}  {int(p['cnt']):>7}  {str(p['supplier_article']):<25}  {p['subject']}  [{label}]")
else:
    print("  WARNING: No products returned from DB.")

# ── 2. Weekly blogger spend — CH6 ─────────────────────────────────────────────
print("\n\n2. WEEKLY BLOGGER SPEND — CH6 СЫВОРОТКА (Nov 2025 – Jun 2026)")
print(SEP2)
print(f"  {'Week Mon':>12}  {'Ad Spend':>10}  {'Prod Cost':>10}  {'Total':>10}  {'N Blogs':>7}")
print(f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*7}")
ch6b = df_blogger[(df_blogger["product"] == "CH6") & (df_blogger["week_start"] >= pd.Timestamp("2025-11-01"))]
ch6_total_spend = 0
for _, r in ch6b.iterrows():
    ch6_total_spend += r["total_spend"]
    print(f"  {str(r['week_start'])[:10]:>12}  {r['ad_spend']:>10,.0f}  {r['product_cost']:>10,.0f}  {r['total_spend']:>10,.0f}  {int(r['n_bloggers']):>7}")
print(f"  {'TOTAL (Nov+)':>12}  {'':>10}  {'':>10}  {ch6_total_spend:>10,.0f}")

print("\n\n3. WEEKLY BLOGGER SPEND — SPF CURACION (Apr–May 2026)")
print(SEP2)
print(f"  {'Week Mon':>12}  {'Ad Spend':>10}  {'Prod Cost':>10}  {'Total':>10}  {'N Blogs':>7}")
print(f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*7}")
spfb = df_blogger[df_blogger["product"] == "SPF"]
spf_total_spend = 0
for _, r in spfb.iterrows():
    spf_total_spend += r["total_spend"]
    print(f"  {str(r['week_start'])[:10]:>12}  {r['ad_spend']:>10,.0f}  {r['product_cost']:>10,.0f}  {r['total_spend']:>10,.0f}  {int(r['n_bloggers']):>7}")
print(f"  {'TOTAL':>12}  {'':>10}  {'':>10}  {spf_total_spend:>10,.0f}")

# ── 4. Weekly WB sales ────────────────────────────────────────────────────────
print("\n\n4. WEEKLY WB ORDERS FROM DATABASE (Nov 2025+)")
print(SEP2)
if len(weekly_sales) > 0:
    for prod_code, prod_label in [("CH6", "CH6 Сыворотка"), ("SPF", "SPF Curacion"), ("Other", "Other")]:
        s = weekly_sales[weekly_sales["product"] == prod_code].sort_values("week_start")
        if len(s) == 0:
            continue
        print(f"\n  [{prod_label}]")
        print(f"  {'Week Mon':>12}  {'Orders':>8}")
        print(f"  {'-'*12}  {'-'*8}")
        for _, r in s.iterrows():
            print(f"  {str(r['week_start'])[:10]:>12}  {int(r['orders']):>8}")
        print(f"  {'TOTAL':>12}  {int(s['orders'].sum()):>8}")
else:
    print("  WARNING: No sales data returned from DB.")

# ── 5. Correlation analysis ───────────────────────────────────────────────────
print("\n\n5. CORRELATION ANALYSIS")
print(SEP2)
print("  Method: Pearson R  |  Lag: spend week W vs orders week W+1")
print("  Window: Nov 2025 onward for CH6;  Apr 2026 onward for SPF")

for prod_code, prod_label, start_d in [
    ("CH6", "CH6 Сыворотка", "2025-11-01"),
    ("SPF", "SPF Curacion", "2026-04-01"),
]:
    print(f"\n  ── {prod_label} ──")
    table, r_same, r_lag = build_corr_table(prod_code, lag_weeks=1, start_date=start_d)

    if table is None or len(table) == 0:
        print("  Insufficient overlap between blogger data and DB sales.")
        continue

    print(f"\n  {'Week Mon':>12}  {'Spend':>10}  {'N':>3}  {'Orders(W)':>10}  {'Orders(W+1)':>12}")
    print(f"  {'-'*12}  {'-'*10}  {'-'*3}  {'-'*10}  {'-'*12}")
    for _, row in table.iterrows():
        nxt = f"{int(row['orders_next_week']):>12}" if not pd.isna(row["orders_next_week"]) else f"{'—':>12}"
        print(f"  {str(row['week_start'])[:10]:>12}  {row['blogger_spend']:>10,.0f}  {int(row['n_bloggers']):>3}  {int(row['orders_same_week']):>10}  {nxt}")

    print(f"\n  Pearson R (same week, no lag):  {interpret_r(r_same)}")
    print(f"  Pearson R (1-week lag):         {interpret_r(r_lag)}")

    # Top 3 by spend
    top3 = table.nlargest(3, "blogger_spend")
    print(f"\n  Top 3 weeks by spend:")
    for _, row in top3.iterrows():
        nxt = f"{int(row['orders_next_week'])}" if not pd.isna(row["orders_next_week"]) else "—"
        print(f"    {str(row['week_start'])[:10]}  spend={row['blogger_spend']:>9,.0f}  orders_W={int(row['orders_same_week'])}  orders_W+1={nxt}")

    # Top 3 by orders
    top3s = table.nlargest(3, "orders_same_week")
    print(f"\n  Top 3 weeks by sales volume:")
    for _, row in top3s.iterrows():
        print(f"    {str(row['week_start'])[:10]}  orders={int(row['orders_same_week'])}  spend_that_week={row['blogger_spend']:>9,.0f}")

# ── 6. Raw DB weekly (full detail) ───────────────────────────────────────────
print("\n\n6. RAW DB WEEKLY SALES — ALL ARTICLES (Nov 2025+)")
print(SEP2)
if weekly_raw:
    print(f"  {'Week':>12}  {'nm_id':>12}  {'Orders':>7}  {'Article':<20}  Subject")
    print(f"  {'-'*12}  {'-'*12}  {'-'*7}  {'-'*20}  {'-'*25}")
    for row in weekly_raw:
        lbl = classify_product(row["supplier_article"], row["subject"])
        print(f"  {row['week_start']:>12}  {str(row['nm_id']):>12}  {int(row['orders']):>7}  {str(row['supplier_article']):<20}  {row['subject']}  [{lbl}]")
else:
    print("  No data.")

# ── 7. Key findings ───────────────────────────────────────────────────────────
print("\n\n7. KEY FINDINGS & SUMMARY")
print(SEP2)

ch6_all_total = df_blogger[df_blogger["product"] == "CH6"]["total_spend"].sum()
spf_all_total = df_blogger[df_blogger["product"] == "SPF"]["total_spend"].sum()
ch6_n = df_blogger[df_blogger["product"] == "CH6"]["n_bloggers"].sum()
spf_n = df_blogger[df_blogger["product"] == "SPF"]["n_bloggers"].sum()

print(f"""
  CH6 Сыворотка Campaign Summary:
    Campaign started:       July 2025
    Total blogger posts:    {int(ch6_n)} (tracked)
    Total spend (Jul-Jun):  {ch6_all_total:>10,.0f} RUB
    Total spend (Nov+):     {ch6_total_spend:>10,.0f} RUB
    Peak weeks:
      2025-07-07: 248,501 RUB — 8 bloggers (launch week, @namennaa 85k RUB)
      2026-01-19: 146,406 RUB — 10 bloggers
      2026-02-23: 158,910 RUB — 9 bloggers
      2026-04-27: 212,436 RUB — 14 bloggers (largest single week)

  SPF Curacion Campaign Summary:
    Campaign started:       April 2026
    Total blogger posts:    {int(spf_n)} (tracked)
    Total spend (Apr-May):  {spf_all_total:>10,.0f} RUB
    Peak weeks:
      2026-04-27: 275,287 RUB — 12 bloggers (peak, @divegandi 40k + @boga.eva_ 30k + @shatullya 30k)
      2026-04-20: 148,840 RUB — 13 bloggers

  Notes on correlation interpretation:
    - Pearson R > +0.5 = meaningful positive relationship
    - R < +0.2 = spend and sales move independently in this window
    - Small correlation may indicate:
        a) Sales lag is longer than 1 week (brand awareness accumulates)
        b) WB orders are partially driven by organic/search traffic, not just bloggers
        c) Barter bloggers create awareness without being counted as spend
        d) WB data starts Nov 2025 — missing Jul-Oct 2025 which was peak blogger spend
    - SPF campaign only 6 weeks of overlap: sample too small for robust correlation
    - Recommend: extend to 2-week and 3-week lag analysis once more data accumulates
""")

print(SEP)
print("  END OF REPORT")
print(SEP)
