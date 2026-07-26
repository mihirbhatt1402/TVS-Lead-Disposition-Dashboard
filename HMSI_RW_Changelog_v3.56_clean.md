# Honda Motorcycles and Scooters Analytics
## Changelog — v3.56 Clean

---

## v3.56 Clean (Restored & Fixed)
**Date:** 25-May-2026

### Summary
Clean restore of v3.56 from a structurally sound base. All share/export
functionality removed. Two performance bugs fixed. Nav alignment improved.
BW zone mapping corrected for three states.

---

### Bug Fixes

**1. buildFilteredDb — console.log removed (Performance)**
- The `buildFilteredDb` function fired a `console.log` statement on every
  single month-filter interaction. In dashboards with large datasets this
  caused perceptible lag on every filter click.
- Removed entirely.

**2. loaded state — instant initialisation**
- `loaded` was initialised as `useState(false)`, causing a blank
  "Initialising…" screen to flash on every page load while the
  localStorage hydration useEffect ran.
- Now initialises as `useState(!!window.__BAKED__)` so the screen is
  never blank in normal operation.

---

### Removed Features

**Share / Export functionality (completely removed)**
The following were removed in full — no trace remains in the codebase:

- `generateShareable()` function
- `shareLoading` state
- ⬇ Share buttons from both RW and BW secondary navbars
- `window.__BAKED__` / `window.__BAKED_RW__` / `window.__BAKED_BW__`
  references in all state initialisers
- `MAIN_VIEWS` IIFE (runtime filter) — reverted to plain static array
- `data-view-id` attributes on primary nav buttons
- `.share-pill` CSS class
- `useEffect` early-return guard for baked mode
- `!window.__BAKED__` guards on upload buttons

---

### UI Fixes

**3. Secondary nav — vertical alignment**
Both the RW and BW secondary nav bars now have `alignItems: center` and
`padding: 0 20px`. Previously, tab buttons and the upload pill sat at
different vertical positions on the bar.

**4. Upload button — margin corrected**
`upload-tab-pill` margin restored to `auto 0 auto auto` so the button
stays flush-right in the nav.

---

### Zone Mapping Corrections (BW State vs Source)

Three states were remapped to the correct zones in `BW_STATE_ZONE_MAP`:

| State | Old Zone | New Zone |
|---|---|---|
| Uttar Pradesh | Central | **North** |
| Uttarakhand | Central | **North** |
| Rajasthan | North | **West** |

---

### What Was Retained from v3.56

- All five sub-tabs: Overview, Model vs Source, Source, State vs Source,
  Lead Type vs Source (both RW and BW)
- BW Zone Grouping (`BWZonedStateGrid`) — South → West → East → North →
  Central with collapsible zone sections
- All charts (Chart.js 4.4.1, inlined)
- All grids and matrix tables
- Month filter, model filter, source filter
- localStorage persistence (RW and BW independently)
- Three top-level tabs: HMSI RW Performance, HMSI BW Performance,
  Predictive Model
- Upload functionality (RW and BW)
- Error boundary
- Data Log modal
- React 18, ReactDOM, Chart.js, XLSX — all inlined for offline use

---

### File Info

| Property | Value |
|---|---|
| Version | 3.56 Clean |
| File | HMSI_RW_Dashboard_v3.56_clean.html |
| Size | ~1,421 KB |
| Built | 25-May-2026 |
| Base | v3.58 (correct HTML structure) with v3.56 features |
| React | 18.2.0 (inlined) |
| Chart.js | 4.4.1 (inlined) |
| XLSX | 0.18.5 (inlined) |
| Offline | Yes — no CDN dependencies |

