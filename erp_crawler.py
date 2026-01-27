import os
import json
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import requests
from playwright.sync_api import sync_playwright
import sys
sys.stdout.reconfigure(encoding="utf-8")

# ==================== CONFIG ====================

ERP_LOGIN_URL = "https://example-erp/"

USERNAME = "USERNAME_LOGIN"
PASSWORD = "PASSWORD_LOGIN"

USERNAME_SELECTOR = "input[name='RealUserName']"
PASSWORD_SELECTOR = "input[name='RealPassword']"
SUBMIT_SELECTOR   = "button[type='submit'], input[type='submit']"

SEARCH_BUTTON_SELECTOR = "button[data-hot-key='Ctrl_f'][data-type-key='keyup']"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR   = os.path.join(SCRIPT_DIR, "ERP_RAW")


RUN_DATE = datetime.now().strftime("%Y-%m-%d")


# ==================== GROUP FOLDER STRUCTURE ====================

GROUP_DIRS = {
    "FABRIC_TRIM": {
        "json_root": os.path.join(BASE_DIR, "FABRIC_TRIM", "JSON"),
    },
    "TREATMENT": {
        "json_root": os.path.join(BASE_DIR, "TREATMENT", "JSON"),
    },
    "TECHNICAL": {
        "json_root": os.path.join(BASE_DIR, "TECHNICAL", "JSON"),
    },
    "COSTING": {
        "json_root": os.path.join(BASE_DIR, "COSTING", "JSON"),
    },
    "CUTTINGDOCKET": {
        "json_root": os.path.join(BASE_DIR, "CUTTINGDOCKET", "JSON"),
    },
    "RAW_DATA": {
        "json_root": os.path.join(BASE_DIR, "RAW_DATA", "JSON"),
    },
    "PO_LIST": {
        "json_root": os.path.join(BASE_DIR, "PO_LIST", "JSON"),
    },
}

# Create Folder + Subfolder by date for group
for g, paths in GROUP_DIRS.items():
    json_root = paths["json_root"]
    os.makedirs (json_root,exist_ok=True)


# ==================== PAYLOAD DEFINITIONS ====================

TREATMENT_COLUMNS = [
    "Row","RequestCode","PurchaseOrderCode","PartnerName","StyleNoInternal",
    "WorkTypeName","SkuNo","StyleName","ProcessTypeName","CustomerNote",
    "Sent_PRT_Date","SentTo_PRT_ByUserName","ExpectReceiveDate","ReceiveDateOf_PRT",
    "ReceivedByUserName","SentSupplierDate","SampleReceiveDate","SampleReceivedByUserName",
    "PRT_SentMerDate","CreatedDate","CreatedByUserName","UpdatedDate","UpdatedByUserName",
    "TreatmentSectionName","StateName",
]

def build_treatment_extra(page_url: str):
    return {
        "CompanyId": "2",
        "PurchaseOrderId": "0",
        "SentTo_PRT_ByUserId": "0",
        "State": "0",
        "KeySearch": "",
        "ByTab": "true",
        "path": urlparse(page_url).path,
    }


def build_treatment_payload(draw, start, length, page_url):
    payload = {
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        **build_treatment_extra(page_url)
    }

    for i, col in enumerate(TREATMENT_COLUMNS):
        payload[f"columns[{i}][data]"] = col
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = "true"
        payload[f"columns[{i}][orderable]"] = "true"
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = "false"

    payload["order[0][column]"] = str(TREATMENT_COLUMNS.index("Sent_PRT_Date"))
    payload["order[0][dir]"] = "desc"

    return payload


# -------- PURCHASE ORDER --------
PURCHASEORDER_COLUMNS = [
    "Row", "CFC", "Code", "PartnerName", "SeasonCode",
    "DropCodes", "WorkTypeName", "JobNumber", "ReceiveDate",
    "StateName", "TotalOrderQty", "TotalSKU", "MerStatusName",
    "AssignUserName", "PersonInChargeUserName", "RejectedReason",
    "RejectedUserName", "RejectedDate", "ApprovedDate",
    "ApprovedUserName", "CreatedDate", "CreatedByUserName",
    "UpdatedDate", "UpdatedByUserName", "24", "25"
]


def build_purchaseorder_payload(draw, start, length, page_url, session_id):
    payload = {
        "draw": draw,
        "start": start,
        "length": length,
        "search[value]": "",
        "search[regex]": False,

        # ===== CONTEXT =====
        "sessionId": session_id or "",
        "CompanyId": 2,
        "path": urlparse(page_url).path,

        # ===== FILTERS (STRICT TYPE) =====
        "StateFromFilter": 0,
        "SeasonId": 0,
        "PartnerId": 0,
        "WorkType": 0,
        "KeySearch": "",
    }

    for i, col in enumerate(PURCHASEORDER_COLUMNS):
        payload[f"columns[{i}][data]"] = col
        payload[f"columns[{i}][name]"] = ""
        payload[f"columns[{i}][searchable]"] = True
        payload[f"columns[{i}][orderable]"] = True
        payload[f"columns[{i}][search][value]"] = ""
        payload[f"columns[{i}][search][regex]"] = False

    payload["order[0][column]"] = 20
    payload["order[0][dir]"] = "desc"

    return payload


# ==================== MERCH CONFIG ====================

MERCH_CONFIG = [ 
    # ------- FABRIC + TRIM ------- 
    {   "name": "fabric_development",
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/",
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormFabricDevelopments.ManageFabricDevelopmentsOfMer&_m=LoadData", 
    }, 
    {   
        "name": "fabric_swatch", 
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormFabricSwatchs.ManageFabricSwatchsOfMer&_m=LoadData", 
    }, 
    { 
        "name": "fabric_labdip", 
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormFabricLabDips.ManageFabricLabDipOfMer&_m=LoadData", 
    }, 
    { 
        "name": "trim_development", 
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.Trims.FormTrimDevelop.ManageTrimDevelopOfMer&_m=LoadData", 
    }, 
    { 
        "name": "trim_swatch", 
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.Trims.FormTrimSwatch.ManageTrimSwatchOfMer&_m=LoadData", 
    }, 
    { 
        "name": "trim_labdip", 
        "group": "FABRIC_TRIM", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.Trims.FormTrimLabDip.ManageTrimLabDipOfMer&_m=LoadData", 
    }, 
    # ------- TREATMENT ------- 
    { 
        "name": "treatment_printing", 
        "group": "TREATMENT", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormTreatments.ManagePrintingsOfMer&_m=LoadData", 
        "payload_type": "TREATMENT",
    }, 
    { 
        "name": "treatment_emb", 
        "group": "TREATMENT", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormTreatments.ManageEMBsOfMer&_m=LoadData", 
        "payload_type": "TREATMENT",
    }, 
    { 
        "name": "treatment_dyewash", 
        "group": "TREATMENT", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormTreatments.ManageDyewashsOfMer&_m=LoadData", 
        "payload_type": "TREATMENT",
    }, 
    { 
        "name": "treatment_print_outsource", 
        "group": "TREATMENT", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormTreatments.ManagePrintOutsourcesOfMer&_m=LoadData", 
        "payload_type": "TREATMENT",
    }, 
    # ------- TECHNICAL ------- 
    { 
        "name": "tech_fabric", 
        "group": "TECHNICAL", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormFabricTechnicals.ManageFabricTechnicalsOfMer&_m=LoadData", 
    }, 
    { 
        "name": "tech_trim", 
        "group": "TECHNICAL", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormTrimTechnicals.ManageTrimTechnicalsOfMer&_m=LoadData", 
    }, 
    { 
        "name": "tech_cmp", 
        "group": "TECHNICAL", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormCMPs.ManageCMPsOfMer&_m=LoadData", 
    }, 
    { 
        "name": "tech_pom", 
        "group": "TECHNICAL", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormPOMs.ManagePOMsOfMer&_m=LoadData", 
    }, 
    # ------- COSTING ------- 
    { 
        "name": "costing", 
        "group": "COSTING", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.Costings.FormCostingSheet.ManageCostingSheetClient&_m=LoadData", 
    }, 
    # ------- CUTTINGDOCKET ------- 
    { 
        "name": "cuttingdocket", 
        "group": "CUTTINGDOCKET", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.CuttingDockets.FormCuttingDockets.ManageCuttingDockets&_m=LoadData", 
    },
    # ------- RAW DATA ------- 
    { 
        "name": "styleoforder", 
        "group": "RAW_DATA", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormPurchaseOrder.ManageListStyleOfOrder&_m=LoadData", 
    }, 
    { 
        "name": "purchaseorder", 
        "group": "RAW_DATA", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.PO.FormPurchaseOrder.ManagePurchaseOrder&_m=LoadData", 
        "payload_type": "PURCHASEORDER",
    }, 
    #-------- FABRIC PLANNING --------
    {
        "name": "mastergroupfabricpotabsofplanningmastergrouppoitems",
        "group": "RAW_DATA",
        "page_url": "https://example-erp/",
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.IPO.For.ForPlanning.MasterGroupFabricPOTabsOfPlanning_MasterGroupPOItems&_m=LoadData",
        "bulk_po": True,
    },
    #-------- TRIM PLANNING --------
    {
        "name": "mastergrouptrimpotabsofplanningmastergrouppoitems",
        "group": "RAW_DATA",
        "page_url": "https://example-erp/",
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.IPO.For.ForPlanning.TrimInspectionReportTabsOfPlanning_Items&_m=LoadData",
        "bulk_po": True,
    },
    # ------- PRODUCTION PLANNING -------
    { 
        "name": "styleroductofplanning", 
        "group": "RAW_DATA", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.IPO.For.ForPlanning.StyleProductOfPlanning&_m=LoadData", 
        "bulk_po": True,
    }, 
    # ------- PO LIST -------     
    {
        "name": "trimpotabsofpurpoitems", 
        "group": "PO_LIST", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.TrimPO.IPO.ForPur.TrimPOTabsOfPur_POItems&_m=LoadData", 
        "bulk_po": True,
    },
    { 
        "name": "fabricpotabsofpurpoitems", 
        "group": "PO_LIST", 
        "page_url": "https://example-erp/", 
        "json_url": "https://example-erp/?_n=Core.Sites.Apps&_o=Web.Projects.ERP.IPO.For.ForPur.FabricPOTabsOfPur_POItems&_m=LoadData", 
        "bulk_po": True,
    },     
]


# ==================== HELPERS ====================

def get_base_name_from_url(url: str):
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    o = qs.get("_o")
    if not o:
        return parsed.path.strip("/").replace("/", "_")
    return o[0].split(".")[-1]


def build_requests_session_from_cookies(cookies):
    s = requests.Session()
    for c in cookies:
        s.cookies.set(
            c["name"],
            c["value"],
            domain=c.get("domain"),
            path=c.get("path", "/")
        )
    return s


def fetch_datatables_all(session, json_url, payload_builder, page_url, page_size=2000):
    all_rows = []
    start = 0
    draw = 1

    while True:
        payload = payload_builder(draw, start, page_size, page_url)
        resp = session.post(json_url, data=payload)
        resp.raise_for_status()

        js = resp.json()
        rows = js.get("data") or js.get("Data", {}).get("objects", [])

        if not rows:
            break

        all_rows.extend(rows)

        records_total = js.get("recordsTotal", 0)
        start += page_size
        draw += 1

        print(f"      fetched {len(all_rows)} / {records_total}")

        if start >= records_total:
            break

        time.sleep(0.5)

    return all_rows


# ==================== MAIN FLOW ====================

def run_crawl(page, session):
    print(f"RUN_DATE = {RUN_DATE}")

    for cfg in MERCH_CONFIG:
        print(f"\n=== PAGE [{cfg['group']}] {cfg['name']} ===")
        page.goto(cfg["page_url"], wait_until="networkidle")
        time.sleep(1)

        # Click search button ERP init session/filter
        try:
            page.wait_for_selector(SEARCH_BUTTON_SELECTOR, timeout=10000)
            page.click(SEARCH_BUTTON_SELECTOR)
            print("  SEARCH clicked")
        except Exception as e:
            print("  SEARCH not found or click failed:", e)

        page.wait_for_timeout(2000)

        rows_all = []
        payload_type = cfg.get("payload_type")

        # ---- BULK PO ----
        if cfg.get("bulk_po"):
            payload = {
                "CompanyId": "2",
                "KeySearch": "",
                "IsWorkTypeBulk": "true",
                "ByTab": "true",
                "path": urlparse(cfg["page_url"]).path
            }
            resp = session.post(cfg["json_url"], data=payload)
            resp.raise_for_status()
            js = resp.json()
            rows_all = js.get("data") or js.get("Data", {}).get("objects", [])

        # ---- TREATMENT ----
        elif payload_type == "TREATMENT":
            print("Fetching JSON (TREATMENT + pagination)")
            rows_all = fetch_datatables_all(
                session,
                cfg["json_url"],
                build_treatment_payload,
                cfg["page_url"]
            )

        # ---- PURCHASE ORDER ----
        elif payload_type == "PURCHASEORDER":
            print("Fetching JSON (PURCHASEORDER FULL)")
            page.wait_for_timeout(2000)

            session_id = page.evaluate("""
                () => {
                    const input = document.querySelector('input[name="sessionId"]');
                    if (input && input.value) return input.value;
                    if (window._sessionId) return window._sessionId;
                    return '';
                }
            """)
            print("   sessionId =", session_id)

            rows_all = fetch_datatables_all(
                session,
                cfg["json_url"],
                lambda d, s, l, p: build_purchaseorder_payload(
                    d, s, l, p, session_id
                ),
                cfg["page_url"]
            )

        # ---- DEFAULT ----
        else:
            resp = session.post(cfg["json_url"])
            resp.raise_for_status()
            js = resp.json()
            rows_all = js.get("data") or js.get("Data", {}).get("objects", [])

        print(f"   received {len(rows_all)} rows")


    # ================= SAVE JSON (OVERWRITE MODE, NO DATE) =================
        json_root = GROUP_DIRS[cfg["group"]]["json_root"]
        os.makedirs(json_root, exist_ok=True)

        base_name = get_base_name_from_url(cfg["json_url"]).lower()
        json_path = os.path.join(json_root, f"{base_name}.json")


        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(rows_all, f, ensure_ascii=False, indent=2)

        print(f"   JSON saved: {json_path}")

    print("\n===== DONE CRAWLING JSON =====")
    print("==============================")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Login
        page.goto(ERP_LOGIN_URL, wait_until="networkidle")
        page.fill(USERNAME_SELECTOR, USERNAME)
        page.fill(PASSWORD_SELECTOR, PASSWORD)
        page.click(SUBMIT_SELECTOR)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        print("Logged in")

        # Build requests session from cookies
        cookies = context.cookies()
        session = build_requests_session_from_cookies(cookies)

        # Run crawl
        run_crawl(page, session)

        browser.close()


if __name__ == "__main__":
    main()
