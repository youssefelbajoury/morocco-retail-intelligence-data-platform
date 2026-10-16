# License decision (owner action required)

> **Status: DECISION PENDING — no `LICENSE` file has been created.**
> This document is a comparison only. No license terms have been selected,
> generated, or applied. Phase 27 is preparation-only and must not invent a
> license. The owner must choose before the repository is published.

The repository currently ships **no `LICENSE` file**, so under default copyright
law it is **"all rights reserved"** — i.e. nobody may copy, modify or redistribute
it even though the source is visible. The README and `CONTRIBUTING.md` state this
explicitly. Choosing an open-source license removes that ambiguity.

---

## 1. What each option means in practice

### Option A — MIT

- **Type:** permissive, short (~170 words), OSI-approved.
- **Grants:** use, copy, modify, merge, publish, distribute, sublicense, sell — for
  any purpose, commercial included.
- **Requires:** keep the copyright notice + license text in copies.
- **Does *not* grant:** any patent license, any trademark rights, any warranty.
- **Best for:** a portfolio project where the goal is maximum legibility and reuse
  with minimum friction (a recruiter or engineer can copy a snippet with no
  thought).
- **Risk:** no explicit patent grant; if a contributor later adds patented code the
  protection is weaker.

### Option B — Apache License 2.0

- **Type:** permissive, longer (~11 KB), OSI-approved.
- **Grants:** same broad use/modify/distribute rights as MIT, **plus an explicit
  patent grant** from contributors, **plus** a trademark clause.
- **Requires:** keep the license + `NOTICE` file, state significant changes, and
  preserve attribution.
- **Does *not* grant:** trademark use, warranty, liability.
- **Best for:** signalling "enterprise-grade"/corporate maturity and providing a
  patent peace clause — often expected in data-platform / infrastructure code.
- **Risk / cost:** more legal text to maintain; the `NOTICE` requirement adds a file
  to manage; slightly heavier for a casual reader.

### Option C — No license (keep as-is / private repository)

- **Type:** default copyright (all rights reserved).
- **Grants:** nothing beyond viewing on the hosting platform.
- **Best for:** keeping the work private, or deliberately withholding reuse rights.
- **Consequence for a *public* portfolio:** reviewers can read but legally may not
  copy/run-modify the code; some portfolio reviewers consider "no license" a
  negative signal. If the repository stays **private**, this is a non-issue.
- **Note:** making a repository public does **not** grant an open-source license by
  itself.

---

## 2. Side-by-side

| Criterion | MIT | Apache-2.0 | No license (private) |
|-----------|-----|------------|----------------------|
| OSI-approved open source | Yes | Yes | No |
| Permission to use commercially | Yes | Yes | No (view only) |
| Patent grant | No | **Yes** | No |
| Trademark clause | No | **Yes** | n/a |
| Attribution / NOTICE duties | Minimal (keep notice) | Notice + `NOTICE` + state changes | n/a |
| License length / maintenance | Very short | Long | n/a |
| Typical audience signal | Simple, permissive | Enterprise / infrastructure | Restrictive |
| Friction to reuse a snippet | Lowest | Low | Prohibited |
| Cartwheeling/portfolio friendliness | High | High | Low (if public) |

None of the three is "wrong". MIT and Apache-2.0 are **compatible with each other**
in the direction that matters here (Apache-2.0 code can be included in an MIT
project; the reverse is the awkward direction). This project contains no
third-party source it must re-license, so either is available.

---

## 3. Official license texts and references

- **MIT** — OSI: <https://opensource.org/license/mit>
  · SPDX: <https://spdx.org/licenses/MIT.html>
  · plain-language: <https://choosealicense.com/licenses/mit/>
- **Apache License 2.0** — canonical: <https://www.apache.org/licenses/LICENSE-2.0>
  · OSI: <https://opensource.org/license/apache-2-0>
  · SPDX: <https://spdx.org/licenses/Apache-2.0.html>
  · plain-language: <https://choosealicense.com/licenses/apache-2.0/>
- **SPDX license list** (machine-readable identifiers): <https://spdx.org/licenses/>
- **No license / all rights reserved** — GitHub guidance:
  <https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository>

---

## 4. Recommendation (non-binding)

The Phase 22 packaging report already suggested **MIT** (maximum legibility for a
portfolio) or **Apache-2.0** (patent grant, an "enterprise infrastructure" signal).
That recommendation stands. For a public Master II-BDCC portfolio piece whose goal
is to be read and reused, **MIT** is the pragmatic default; choose **Apache-2.0** if
you want the explicit patent grant and a more formal posture. If you intend to keep
the repository **private**, selecting no license is also defensible.

**The owner must pick one; Phase 27 has not and will not choose for you.**

---

## 5. What Phase 27 will do once the owner decides (not executed yet)

1. Create the chosen `LICENSE` file verbatim from the official text above (no
   edits, no added clauses).
2. Set the SPDX `license` field in `pyproject.toml` (e.g. `license = "MIT"` or
   `license = "Apache-2.0"`).
3. Replace the README **License** badge (`license-undecided`) and the
   "License & data disclaimer" section with the chosen license.
4. Update `CONTRIBUTING.md` §License to name the chosen license.
5. For Apache-2.0 only: add an initial `NOTICE` file.

Until then, **no `LICENSE` file exists and no license is asserted beyond default
copyright.**

---

## 6. Decision record

| Field | Value |
|-------|-------|
| Decision owner | Project owner (repo owner) |
| Options | MIT · Apache-2.0 · no license / private |
| Decision | **PENDING** |
| Date chosen | — |
| Resulting files to add | `LICENSE` (+ `NOTICE` only if Apache-2.0) |
