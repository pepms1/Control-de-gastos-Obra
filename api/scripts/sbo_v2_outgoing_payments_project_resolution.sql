-- Outgoing payments V2 project resolution
-- Keeps document-level project logic and adds payment-level project from OVPM.TransId -> JDT1.Project.
-- Final raw project should map to resolved_project_* for egresos.

WITH payment_project_ranked AS (
  SELECT
    p.DocEntry AS payment_docentry,
    j.Project AS payment_project_code,
    SUM(ABS(ISNULL(j.Debit, 0) - ISNULL(j.Credit, 0))) AS weight,
    MIN(j.Line_ID) AS min_line_id,
    ROW_NUMBER() OVER (
      PARTITION BY p.DocEntry
      ORDER BY
        SUM(ABS(ISNULL(j.Debit, 0) - ISNULL(j.Credit, 0))) DESC,
        MIN(j.Line_ID) ASC
    ) AS rn
  FROM OVPM p
  INNER JOIN JDT1 j ON j.TransId = p.TransId
  WHERE NULLIF(LTRIM(RTRIM(j.Project)), '') IS NOT NULL
  GROUP BY p.DocEntry, j.Project
),
payment_project AS (
  SELECT
    r.payment_docentry,
    r.payment_project_code,
    prj.PrjName AS payment_project_name
  FROM payment_project_ranked r
  LEFT JOIN OPRJ prj ON prj.PrjCode = r.payment_project_code
  WHERE r.rn = 1
)
SELECT
  base.*,
  base.document_project_code,
  base.document_project_name,
  pp.payment_project_code,
  pp.payment_project_name,
  CASE
    WHEN NULLIF(LTRIM(RTRIM(pp.payment_project_code)), '') IS NOT NULL THEN pp.payment_project_code
    ELSE base.document_project_code
  END AS resolved_project_code,
  CASE
    WHEN NULLIF(LTRIM(RTRIM(pp.payment_project_code)), '') IS NOT NULL THEN pp.payment_project_name
    ELSE base.document_project_name
  END AS resolved_project_name,
  CASE
    WHEN NULLIF(LTRIM(RTRIM(pp.payment_project_code)), '') IS NOT NULL THEN 'payment_jdt1'
    ELSE 'document'
  END AS project_resolution_source,
  -- For outgoing payments only: raw project is resolved project.
  CASE
    WHEN NULLIF(LTRIM(RTRIM(pp.payment_project_code)), '') IS NOT NULL THEN pp.payment_project_code
    ELSE base.document_project_code
  END AS raw_project_code,
  CASE
    WHEN NULLIF(LTRIM(RTRIM(pp.payment_project_code)), '') IS NOT NULL THEN pp.payment_project_name
    ELSE base.document_project_name
  END AS raw_project_name
FROM (
  -- Existing V2 outgoing payment query should expose:
  -- * payment_docentry
  -- * document_project_code (PCH1.Project -> OPCH.Project -> first non-empty PCH1.Project)
  -- * document_project_name
  SELECT
    q.*,
    q.payment_docentry,
    q.document_project_code,
    q.document_project_name
  FROM your_existing_outgoing_payments_v2_query q
) base
LEFT JOIN payment_project pp ON pp.payment_docentry = base.payment_docentry;
