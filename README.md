# Insurance Product Pricing Validator

## Overview

This solution validates and corrects insurance product pricing according to established business rules. The system ensures that all prices maintain proper relationships across product types, coverage variants, and deductible levels.

## Insurance Products

The pricing validator handles three main insurance product categories:

### Product Hierarchy

1. **MTPL (Motor Third Party Liability)** - Mandatory basic coverage at the lowest price point
2. **Limited Casco** - Extends MTPL with additional risk coverage (theft, etc.)
3. **Casco** - Full comprehensive coverage including own vehicle damages

The pricing relationship is: **MTPL < Limited Casco < Casco**

### Coverage Variants

For Limited Casco and Casco products, customers can choose from four coverage variants:

- **Compact** - Most economical option
- **Basic** - Standard coverage level
- **Comfort** - Enhanced coverage
- **Premium** - Maximum coverage

Typical pricing: **Compact/Basic < Comfort < Premium**

*Note: Compact and Basic pricing may vary relative to each other as they serve different customer segments.*

### Deductible Options

Customers can select their preferred deductible amount (the cost they bear in case of a claim):

- **100€** - No out-of-pocket cost, full premium
- **200€** - Moderate deductible
- **500€** - Higher deductible, lower premium

Higher deductibles result in lower premiums: **100€ > 200€ > 500€ (in price)**

---

## Validation Rules

The validator enforces three primary pricing constraints:

### Rule 1: Product Level Consistency

Ensures that base MTPL is the cheapest option, followed by Limited Casco, then Casco.

**Logic:** For identical variants and deductibles, prices must satisfy:
```
MTPL < Limited Casco < Casco
```

**Example:** 
- MTPL: 400€
- Limited Casco (Basic, 100€ deductible): 900€
- Casco (Basic, 100€ deductible): 1050€

### Rule 2: Variant Hierarchy Consistency

Within the same product and deductible, coverage variants must be ordered by price to reflect their enhanced benefits.

**Logic:** For a fixed product and deductible:
```
Basic < Comfort < Premium
```

**Example (Limited Casco, 100€ deductible):**
- Basic: 900€
- Comfort: 950€
- Premium: 1100€

### Rule 3: Deductible Inverse Relationship

Higher deductibles reduce the premium because customers assume more risk.

**Logic:** For a fixed product and variant:
```
100€ deductible > 200€ deductible > 500€ deductible (in price)
```

**Example (Limited Casco, Basic):**
- 100€ deductible: 900€
- 200€ deductible: 780€
- 500€ deductible: 600€

---

## Inconsistency Detection & Fixing

The validator automatically detects violations and applies corrections based on business-appropriate adjustments.

### Detected Issues

The system identifies the following types of violations:

1. **Product hierarchy violations** - A lower-tier product priced higher than a higher-tier product
2. **Variant order violations** - A lower-variant priced at or above a higher-variant
3. **Deductible violations** - A higher deductible (lower risk for insurer) priced higher than a lower deductible

### Automatic Corrections

When violations are detected, prices are adjusted using these reference adjustments:

- **Deductible adjustment:** ~10% per deductible step
  - Moving from 100€ to 200€: reduce by ~10%
  - Moving from 200€ to 500€: reduce by ~10%

- **Variant adjustment:** ~7% per variant level
  - Compact/Basic as baseline (0%)
  - Comfort: +7% above Basic
  - Premium: +7% above Comfort

- **Product adjustment:** ~15% per tier
  - Limited Casco typically 15-20% above MTPL
  - Casco typically 15% above Limited Casco

### Correction Strategy

The validator prioritizes the input prices as ground truth and adjusts other conflicting prices:
- If a product is underpriced relative to another, the lower-priced one is reduced further
- If a product is expected to be more expensive, the higher-tier product is increased
- Explanations are provided for each correction, documenting the business reasoning

---

## Algorithm Workflow

### Step 1: Parse Product Keys

The input dictionary uses string keys that encode product, variant, and deductible information:
- `mtpl` → MTPL base product
- `limited_casco_basic_100` → Limited Casco, Basic variant, 100€ deductible
- `casco_premium_500` → Casco, Premium variant, 500€ deductible

Each key is parsed into structured components for validation.

### Step 2: Extract and Group Prices

Prices are grouped by:
- Product type (MTPL, Limited Casco, Casco)
- Variant level (Compact, Basic, Comfort, Premium)
- Deductible amount (100, 200, 500)

This allows systematic comparison across dimensions.

### Step 3: Validate and Fix

#### Product Level Validation
For each combination of variant and deductible, compare prices across product types:
- MTPL should be the lowest
- Limited Casco should be higher than MTPL
- Casco should be the highest

If violations occur, the highest-tier product is increased to maintain prominence.

#### Variant Hierarchy Validation
For each product and deductible, ensure variants follow increasing prices:
- Comfort should exceed Basic
- Premium should exceed Comfort

Violations trigger price increases on the higher variant.

#### Deductible Inverse Validation
For each product and variant, ensure higher deductibles have lower prices:
- 200€ deductible should be ~10% cheaper than 100€
- 500€ deductible should be ~10% cheaper than 200€

Violations are corrected by reducing the higher deductible price.

### Step 4: Report Results

The function returns:
- **fixed_prices** - Corrected pricing dictionary
- **issues** - List of detected violations
- **explanations** - Business rationale for each correction applied

---

## Implementation Highlights

### Design Principles

1. **Simplicity** - Single-file solution with clear logic flow
2. **Maintainability** - Helper functions for grouping and comparison
3. **Business-Focused** - Adjustments align with insurance industry practices
4. **Non-Destructive** - Input is deep-copied; original data remains unchanged

### Key Functions

- `parse_key(key)` - Extracts product, variant, and deductible from dictionary keys
- `validate_and_fix(prices)` - Main validation and correction function
- `find_keys()` - Helper to retrieve keys matching specific criteria

### Data Integrity

- Prices are validated against all three rules simultaneously
- Corrections consider business context, not just mathematical relationships
- Each fix is documented with its rationale

---

## Example Usage

```python
input_prices = {
    "mtpl": 400,
    "limited_casco_basic_100": 900,
    "limited_casco_basic_200": 780,
    "limited_casco_basic_500": 600,
    "casco_basic_100": 1050,
    "casco_basic_200": 950,
    "casco_basic_500": 780,
}

result = validate_and_fix(input_prices)

# Fixed prices with corrections applied
print(result["fixed_prices"])

# Issues that were detected
print(result["issues"])

# Explanations for each fix
print(result["explanations"])
```

---

## Business Context

This validator ensures that the pricing structure:

- **Maintains competitiveness** - Correct ordering prevents customer confusion and unfair pricing
- **Reflects risk assessment** - Higher deductibles reduce insurer risk, justifying lower premiums
- **Supports coverage hierarchy** - Premium variants with better coverage cost more
- **Enables market segmentation** - Three distinct products serve different customer needs

The automatic correction feature ensures that pricing remains internally consistent and aligned with industry best practices.
