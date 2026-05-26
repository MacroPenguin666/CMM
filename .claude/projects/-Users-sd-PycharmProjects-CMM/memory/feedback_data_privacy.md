---
name: feedback-data-privacy
description: User requires all underlying data to be private — dashboard viewable but raw DB inaccessible
metadata:
  type: feedback
---

No one can access the underlying data. Users can look at the dashboard AT MAXIMUM but CANNOT access ANY of the underlying data.

**Why:** User explicitly stated this when choosing Supabase. The original plan exposed a Supabase anon key in frontend JS, which would let anyone with DevTools query every table directly. User rejected that.

**How to apply:** Never expose database credentials (anon key, service key, connection strings) in frontend code. Use a build-step architecture: backend writes to private DB → server-side export generates display-ready JSON → static site serves JSON only. The Supabase anon key must NOT appear in any client-side code. Only the service-role key (in `.env` / GitHub Secrets) touches the DB.