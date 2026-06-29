#!/usr/bin/env python3
"""Generate schema-conformant SSIS .dtsx sample packages that BladeBridge converts.

Real SSIS (SSDT) structure is reproduced: DTS namespace + refIds, Execute SQL Tasks
using the SQLTask: namespace with SqlStatementSource, and Data Flow pipelines with
componentClassID components wired by <paths> (startId/endId). Logic lives mostly in
rich OLE DB Source SELECTs and Execute SQL Tasks, which BladeBridge converts fully.
"""
from pathlib import Path
from xml.sax.saxutils import escape

# Write to ../sample_assets relative to this script (scripts/), so it works from anywhere.
OUT = Path(__file__).resolve().parent.parent / "sample_assets"


def esql(refid, name, sql):
    return (f'    <DTS:Executable DTS:refId="{refid}" DTS:ExecutableType="Microsoft.ExecuteSQLTask" '
            f'DTS:ObjectName="{name}">\n'
            f'      <DTS:ObjectData>\n'
            f'        <SQLTask:SqlTaskData SQLTask:SqlStatementSource="{escape(sql, {chr(34): "&quot;"})}" '
            f'xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask" />\n'
            f'      </DTS:ObjectData>\n'
            f'    </DTS:Executable>\n')


def ctrl_task(refid, name, exec_type):
    """A control-flow task with no convertible body — used to inject components that
    BladeBridge cannot convert (Send Mail, Script, ForEach, …) so a package becomes a
    realistic 'needs manual review' case (the other tasks still convert)."""
    return (f'    <DTS:Executable DTS:refId="{refid}" DTS:ExecutableType="{exec_type}" '
            f'DTS:ObjectName="{name}">\n      <DTS:ObjectData/>\n    </DTS:Executable>\n')


def _cols(ref, names):
    return "".join(f'<outputColumn refId="{ref}.Columns[{n}]" name="{n}"/>' for n in names)


def source(p, key, sql, cols, name="OLE DB Source"):
    ref = f"{p}\\{key}"
    out = f"{ref}.Outputs[out]"
    return ref, out, (
        f'<component refId="{ref}" name="{name}" componentClassID="Microsoft.OLEDBSource">'
        f'<properties><property name="AccessMode">2</property>'
        f'<property name="SqlCommand">{escape(sql)}</property></properties>'
        f'<outputs><output refId="{out}" name="OLE DB Source Output"><outputColumns>'
        f'{_cols(out, cols)}</outputColumns></output></outputs></component>')


def lookup(p, key, sql, name="Lookup"):
    ref = f"{p}\\{key}"
    out = f"{ref}.Outputs[Lookup Match Output]"
    return ref, out, (
        f'<component refId="{ref}" name="{name}" componentClassID="Microsoft.Lookup">'
        f'<properties><property name="SqlCommand">{escape(sql)}</property></properties>'
        f'<inputs><input refId="{ref}.Inputs[in]" name="Lookup Input"><inputColumns/></input></inputs>'
        f'<outputs><output refId="{out}" name="Lookup Match Output"><outputColumns/></output></outputs></component>')


def derived(p, key, col, expr, name="Derived Column"):
    ref = f"{p}\\{key}"
    out = f"{ref}.Outputs[out]"
    return ref, out, (
        f'<component refId="{ref}" name="{name}" componentClassID="Microsoft.DerivedColumn">'
        f'<inputs><input refId="{ref}.Inputs[in]" name="Derived Column Input"><inputColumns/></input></inputs>'
        f'<outputs><output refId="{out}" name="Derived Column Output"><outputColumns>'
        f'<outputColumn refId="{out}.Columns[{col}]" name="{col}"><properties>'
        f'<property name="Expression">{escape(expr)}</property>'
        f'<property name="FriendlyExpression">{escape(expr)}</property></properties></outputColumn>'
        f'</outputColumns></output></outputs></component>')


def cond_split(p, key, outname, expr, name="Conditional Split"):
    ref = f"{p}\\{key}"
    out = f"{ref}.Outputs[{outname}]"
    return ref, out, (
        f'<component refId="{ref}" name="{name}" componentClassID="Microsoft.ConditionalSplit">'
        f'<inputs><input refId="{ref}.Inputs[in]" name="Conditional Split Input"><inputColumns/></input></inputs>'
        f'<outputs><output refId="{out}" name="{outname}"><properties>'
        f'<property name="Expression">{escape(expr)}</property>'
        f'<property name="FriendlyExpression">{escape(expr)}</property></properties></output></outputs></component>')


def dest(p, key, table, name="OLE DB Destination"):
    ref = f"{p}\\{key}"
    inp = f"{ref}.Inputs[in]"
    return ref, inp, (
        f'<component refId="{ref}" name="{name}" componentClassID="Microsoft.OLEDBDestination">'
        f'<properties><property name="OpenRowset">{escape(table)}</property></properties>'
        f'<inputs><input refId="{inp}" name="OLE DB Destination Input"><inputColumns/></input></inputs>'
        f'</component>')


def path(p, n, start, end):
    return f'<path refId="{p}.Paths[{n}]" name="{n}" startId="{start}" endId="{end}"/>'


def pipeline(p, refid, name, components, paths):
    return (f'    <DTS:Executable DTS:refId="{refid}" DTS:ExecutableType="Microsoft.Pipeline" '
            f'DTS:ObjectName="{name}">\n'
            f'      <DTS:ObjectData><pipeline><components>{components}</components>'
            f'<paths>{paths}</paths></pipeline></DTS:ObjectData>\n'
            f'    </DTS:Executable>\n')


def package(obj_name, body):
    return (f'<?xml version="1.0"?>\n'
            f'<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts" '
            f'DTS:refId="Package" DTS:ExecutableType="Microsoft.Package" DTS:ObjectName="{obj_name}">\n'
            f'  <DTS:Executables>\n{body}  </DTS:Executables>\n'
            f'</DTS:Executable>\n')


def build(filename, obj_name, executables):
    OUT.joinpath(filename).write_text(package(obj_name, "".join(executables)), encoding="utf-8")
    print("wrote", filename)


# ── 05 — Customer load: source query → dim, then refresh metrics ──────────────
P = "Package\\DFT Customer"
sref, sout, scomp = source(P, "Src Customers",
    "SELECT CustomerID, FirstName + ' ' + LastName AS FullName, "
    "ISNULL(Email,'N/A') AS Email, CONVERT(VARCHAR, CreatedDate, 120) AS CreatedDate "
    "FROM dbo.Customer WHERE IsActive = 1 AND CreatedDate >= DATEADD(MONTH, -3, GETDATE())",
    ["CustomerID", "FullName", "Email", "CreatedDate"])
dref, din, dcomp = dest(P, "Dst dim_customer", "[dw].[dim_customer]")
build("05_ssis_customer_load.dtsx", "ETL_Load_Customer_Dim", [
    pipeline(P, P, "DFT Customer", scomp + dcomp, path(P, "src2dst", sout, din)),
    esql("Package\\Refresh Customer Metrics", "Refresh Customer Metrics",
         "UPDATE m SET m.LastOrderDate = o.MaxDate, m.TotalSpend = o.Total "
         "FROM dbo.CustomerMetrics m JOIN (SELECT CustomerID, MAX(OrderDate) MaxDate, "
         "SUM(TotalDue) Total FROM dbo.SalesOrderHeader GROUP BY CustomerID) o "
         "ON o.CustomerID = m.CustomerID;"),
])

# ── 06 — Finance sync: truncate stage, load via source, merge to fact ─────────
P = "Package\\DFT Finance"
sref, sout, scomp = source(P, "Src GL",
    "SELECT a.AccountCode, a.AccountName, t.Amount, t.PostingDate "
    "FROM fin.GLTransaction t JOIN fin.Account a ON a.AccountID = t.AccountID "
    "WHERE t.PostingDate >= DATEADD(DAY, -1, GETDATE())",
    ["AccountCode", "AccountName", "Amount", "PostingDate"])
dref, din, dcomp = dest(P, "Dst stage", "[staging].[finance]")
build("06_ssis_finance_sync.dtsx", "ETL_Finance_Sync", [
    esql("Package\\Truncate Stage", "Truncate Stage", "TRUNCATE TABLE staging.finance;"),
    pipeline(P, P, "DFT Finance", scomp + dcomp, path(P, "src2dst", sout, din)),
    esql("Package\\Merge To Fact", "Merge To Fact",
         "MERGE INTO dw.fact_finance t USING staging.finance s "
         "ON t.AccountCode = s.AccountCode AND t.PostingDate = s.PostingDate "
         "WHEN MATCHED THEN UPDATE SET t.Amount = s.Amount "
         "WHEN NOT MATCHED THEN INSERT (AccountCode, AccountName, Amount, PostingDate) "
         "VALUES (s.AccountCode, s.AccountName, s.Amount, s.PostingDate);"),
])

# ── 07 — Order enrichment: enrich via JOIN/CASE in the source query → dest ─────
# (Enrichment/lookup expressed as SQL in the source SELECT — converts cleanly.)
P = "Package\\DFT Orders"
sref, sout, scomp = source(P, "Src Orders Enriched",
    "SELECT d.OrderID, d.CustomerID, s.SegmentName, "
    "d.Quantity * d.UnitPrice AS LineAmount, "
    "CASE WHEN d.Quantity * d.UnitPrice > 1000 THEN 'HighValue' ELSE 'Standard' END AS ValueBand "
    "FROM dbo.SalesOrderDetail d "
    "LEFT JOIN dbo.CustomerSegment s ON s.CustomerID = d.CustomerID",
    ["OrderID", "CustomerID", "SegmentName", "LineAmount", "ValueBand"])
dref, din, dcomp = dest(P, "Dst fact_orders", "[dw].[fact_order_enriched]")
build("07_ssis_order_enrichment.dtsx", "ETL_Order_Enrichment", [
    pipeline(P, P, "DFT Orders", scomp + dcomp, path(P, "src2dst", sout, din)),
    esql("Package\\Audit", "Audit Row Count",
         "INSERT INTO control.load_audit (package_name, loaded_at) "
         "SELECT 'ETL_Order_Enrichment', GETDATE();"),
])

# ── 08 — Daily batch orchestrator: sequence of Execute SQL Tasks + one DFT ─────
P = "Package\\DFT Stage Sales"
sref, sout, scomp = source(P, "Src Sales",
    "SELECT SalesID, ProductID, Amount, SaleDate FROM dbo.Sales WHERE SaleDate = CAST(GETDATE() AS DATE)",
    ["SalesID", "ProductID", "Amount", "SaleDate"])
dref, din, dcomp = dest(P, "Dst stage_sales", "[staging].[sales]")
# Deliberately HARD: an FTP Task (fetch the daily file) and a Send Mail Task (notify) —
# both listed as UNSUPPORTED in the Lakebridge SSIS docs. The SQL tasks + data flow
# still convert, so this is a realistic "partially converted — needs manual review" case.
build("08_ssis_daily_batch_orchestrator.dtsx", "ETL_Daily_Batch", [
    ctrl_task("Package\\Fetch File", "Fetch Daily File", "Microsoft.FtpTask"),
    esql("Package\\Truncate Staging", "Truncate Staging", "TRUNCATE TABLE staging.sales;"),
    pipeline(P, P, "DFT Stage Sales", scomp + dcomp, path(P, "src2dst", sout, din)),
    esql("Package\\Load Fact", "Load Fact",
         "INSERT INTO dw.fact_sales (ProductID, Amount, SaleDate) "
         "SELECT ProductID, SUM(Amount), SaleDate FROM staging.sales GROUP BY ProductID, SaleDate;"),
    esql("Package\\Update Control", "Update Control Table",
         "UPDATE control.batch SET last_run = GETDATE(), rows_loaded = "
         "(SELECT COUNT(*) FROM staging.sales) WHERE batch_name = 'DailySales';"),
    ctrl_task("Package\\Notify", "Notify On Complete", "Microsoft.SendMailTask"),
])

# ── 09 — Product SCD2: expire changed rows (SQL), then load new versions ───────
# (SCD2 change detection expressed as a JOIN in the source query.)
P = "Package\\DFT Product"
sref, sout, scomp = source(P, "Src Changed Products",
    "SELECT p.ProductID, p.ProductName, p.ListPrice, p.Category "
    "FROM dbo.Product p "
    "LEFT JOIN dw.dim_product d ON d.ProductID = p.ProductID AND d.IsCurrent = 1 "
    "WHERE d.ProductID IS NULL OR d.ListPrice <> p.ListPrice",
    ["ProductID", "ProductName", "ListPrice", "Category"])
dref, din, dcomp = dest(P, "Dst new_version", "[dw].[dim_product]")
build("09_ssis_product_scd2.dtsx", "ETL_Product_SCD2", [
    esql("Package\\Expire Old", "Expire Changed Rows",
         "UPDATE dw.dim_product SET IsCurrent = 0, EndDate = GETDATE() "
         "WHERE IsCurrent = 1 AND ProductID IN ("
         "SELECT p.ProductID FROM dbo.Product p JOIN dw.dim_product d "
         "ON d.ProductID = p.ProductID AND d.IsCurrent = 1 WHERE d.ListPrice <> p.ListPrice);"),
    pipeline(P, P, "DFT Product", scomp + dcomp, path(P, "src2dst", sout, din)),
])

# ── 10 — Data quality checks: DQ rule Execute SQL Tasks + quarantine pipeline ──
# (Invalid-row filter expressed as a WHERE in the source query → quarantine table.)
P = "Package\\DFT DQ"
sref, sout, scomp = source(P, "Src Invalid Rows",
    "SELECT RecordID, CustomerEmail, OrderAmount, "
    "CASE WHEN CustomerEmail IS NULL THEN 'null_email' "
    "WHEN OrderAmount < 0 THEN 'negative_amount' END AS dq_reason "
    "FROM staging.raw_orders "
    "WHERE CustomerEmail IS NULL OR OrderAmount < 0",
    ["RecordID", "CustomerEmail", "OrderAmount", "dq_reason"])
dref, din, dcomp = dest(P, "Dst quarantine", "[dq].[quarantine]")
# Deliberately HARD: a Web Service Task (call an external validation API) — listed as
# UNSUPPORTED in the Lakebridge SSIS docs. The DQ SQL tasks + quarantine data flow still
# convert, so this is a realistic "partially converted — needs manual review" case.
build("10_ssis_data_quality_checks.dtsx", "ETL_Data_Quality", [
    esql("Package\\Null Check", "Null Check",
         "INSERT INTO dq.results (rule, failed_rows) SELECT 'null_email', COUNT(*) "
         "FROM staging.raw_orders WHERE CustomerEmail IS NULL;"),
    esql("Package\\Range Check", "Range Check",
         "INSERT INTO dq.results (rule, failed_rows) SELECT 'negative_amount', COUNT(*) "
         "FROM staging.raw_orders WHERE OrderAmount < 0;"),
    ctrl_task("Package\\Validate API", "Validate via Web Service", "Microsoft.WebServiceTask"),
    pipeline(P, P, "DFT DQ", scomp + dcomp, path(P, "src2dst", sout, din)),
])
print("done")
