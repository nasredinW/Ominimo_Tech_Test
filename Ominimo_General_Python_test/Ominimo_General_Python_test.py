import copy

def parse_key(key):
    parts = key.split("_")
    
    if key == "mtpl":
        return {
            "product": "mtpl",
            "variant": None,
            "deductible": None
        }
    
    # product can be 2 words (limited_casco)
    if parts[0] == "limited":
        product = "limited_casco"
        variant = parts[2]
        deductible = int(parts[3])
    else:
        product = parts[0]  # casco
        variant = parts[1]
        deductible = int(parts[2])
    
    return {
        "product": product,
        "variant": variant,
        "deductible": deductible
    }


def validate_and_fix(prices):
    prices = copy.deepcopy(prices)
    parsed = {k: parse_key(k) for k in prices}
    
    issues = []
    explanations = []
    
    # -----------------------------
    # Helper: group data
    # -----------------------------
    def find_keys(product=None, variant=None, deductible=None):
        result = []
        for k, v in parsed.items():
            if product is not None and v["product"] != product:
                continue
            if variant is not None and v["variant"] != variant:
                continue
            if deductible is not None and v["deductible"] != deductible:
                continue
            result.append(k)
        return result

    # -----------------------------
    # 1. PRODUCT RULE
    # MTPL < Limited Casco < Casco
    # -----------------------------
    for k1 in prices:
        for k2 in prices:
            p1 = parsed[k1]
            p2 = parsed[k2]

            if p1["variant"] == p2["variant"] and p1["deductible"] == p2["deductible"]:
                
                # limited vs casco
                if p1["product"] == "limited_casco" and p2["product"] == "casco":
                    if prices[k1] >= prices[k2]:
                        issues.append(f"{k1} should be cheaper than {k2}")
                        
                        new_price = round(prices[k1] * 1.15)
                        explanations.append(
                            f"Increased {k2} from {prices[k2]} to {new_price} (Casco should be more expensive than Limited Casco)"
                        )
                        prices[k2] = new_price

                # mtpl vs others
                if p1["product"] == "mtpl" and p2["product"] != "mtpl":
                    if prices[k1] >= prices[k2]:
                        issues.append(f"MTPL should be cheapest compared to {k2}")
                        
                        new_price = round(prices[k1] * 0.8)
                        explanations.append(
                            f"Reduced MTPL from {prices[k1]} to {new_price} to ensure it is cheapest"
                        )
                        prices[k1] = new_price

    # -----------------------------
    # 2. VARIANT RULE
    # Basic < Comfort < Premium
    # (ignore Compact vs Basic)
    # -----------------------------
    variant_order = ["basic", "comfort", "premium"]

    for product in ["limited_casco", "casco"]:
        for deductible in [100, 200, 500]:
            for i in range(len(variant_order) - 1):
                v1 = variant_order[i]
                v2 = variant_order[i + 1]

                k1_list = find_keys(product, v1, deductible)
                k2_list = find_keys(product, v2, deductible)

                for k1 in k1_list:
                    for k2 in k2_list:
                        if prices[k1] >= prices[k2]:
                            issues.append(f"{k2} should be more expensive than {k1}")
                            
                            new_price = round(prices[k1] * 1.07)
                            explanations.append(
                                f"Increased {k2} from {prices[k2]} to {new_price} (variant hierarchy)"
                            )
                            prices[k2] = new_price

    # -----------------------------
    # 3. DEDUCTIBLE RULE
    # 100 > 200 > 500 (price-wise)
    # -----------------------------
    deductibles = [100, 200, 500]

    for product in ["limited_casco", "casco"]:
        variants = set(v["variant"] for v in parsed.values() if v["product"] == product)

        for variant in variants:
            for i in range(len(deductibles) - 1):
                d1 = deductibles[i]
                d2 = deductibles[i + 1]

                k1_list = find_keys(product, variant, d1)
                k2_list = find_keys(product, variant, d2)

                for k1 in k1_list:
                    for k2 in k2_list:
                        if prices[k1] <= prices[k2]:
                            issues.append(f"{k2} should be cheaper than {k1}")
                            
                            new_price = round(prices[k1] * (1 - 0.10))
                            explanations.append(
                                f"Reduced {k2} from {prices[k2]} to {new_price} (higher deductible → lower price)"
                            )
                            prices[k2] = new_price

    return {
        "fixed_prices": prices,
        "issues": issues,
        "explanations": explanations
    }


# -----------------------------
# ✅ TEST WITH PROVIDED EXAMPLE
# -----------------------------
input_prices = { 
"mtpl": 400, 
"limited_casco_compact_100": 820, 
"limited_casco_compact_200": 760, 
"limited_casco_compact_500": 650, 
"limited_casco_basic_100": 900, 
"limited_casco_basic_200": 780, 
"limited_casco_basic_500": 600, 
"limited_casco_comfort_100": 950, 
"limited_casco_comfort_200": 870, 
"limited_casco_comfort_500": 720, 
"limited_casco_premium_100": 1100, 
"limited_casco_premium_200": 980, 
"limited_casco_premium_500": 800, 
"casco_compact_100": 750, 
"casco_compact_200": 700, 
"casco_compact_500": 620, 
"casco_basic_100": 830, 
"casco_basic_200": 760, 
"casco_basic_500": 650, 
"casco_comfort_100": 900, 
"casco_comfort_200": 820, 
"casco_comfort_500": 720, 
"casco_premium_100": 1050, 
"casco_premium_200": 950, 
"casco_premium_500": 780 
}

result = validate_and_fix(input_prices)

print("Fixed Prices:")
for k, v in result["fixed_prices"].items():
    print(f"{k}: {v}")

print("\nIssues Found:")
for i in result["issues"]:
    print("-", i)

print("\nFix Explanations:")
for e in result["explanations"]:
    print("-", e)