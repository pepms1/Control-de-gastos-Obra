/**
 * @typedef {Object} NormalizedSapMeta
 * @property {string|null} movementType
 * @property {string|null} sourceType
 * @property {number|null} paymentDocEntry
 * @property {number|null} paymentNum
 * @property {number|null} invoiceDocEntry
 * @property {number|null} invoiceNum
 * @property {string|null} externalDocNum
 * @property {string|null} movementDate
 * @property {string|null} invoiceDate
 * @property {number|null} montoAplicado
 * @property {number|null} invoiceSubtotal
 * @property {number|null} invoiceIva
 * @property {number|null} invoiceTotal
 * @property {string|null} paymentCurrency
 * @property {string|null} invoiceCurrency
 * @property {string|null} cardCode
 * @property {string|null} businessPartner
 * @property {string|null} categoryHintCode
 * @property {string|null} categoryHintName
 * @property {string|null} rawProjectCode
 * @property {string|null} rawProjectName
 * @property {string|null} documentProjectCode
 * @property {string|null} documentProjectName
 * @property {string|null} paymentProjectCode
 * @property {string|null} paymentProjectName
 * @property {string|null} projectResolutionSource
 * @property {string|null} normalizedProjectName
 * @property {string|null} sourceDb
 * @property {string|null} sourceSbo
 * @property {string|null} sourceSboMode
 */

/**
 * @typedef {Object} NormalizedTransactionViewModel
 * @property {string} id
 * @property {string|null} projectId
 * @property {string} projectDisplayName
 * @property {string|null} date
 * @property {string} description
 * @property {string} supplierName
 * @property {'EXPENSE'|'INCOME'|string|null} type
 * @property {number|null} amount
 * @property {number|null} subtotal
 * @property {number|null} iva
 * @property {number|null} totalFactura
 * @property {string|null} categoryCode
 * @property {string|null} categoryName
 * @property {string|null} source
 * @property {string|null} sourceDb
 * @property {string|null} sourceSbo
 * @property {boolean} isSapSbo
 * @property {string} sapBadgeLabel
 * @property {NormalizedSapMeta|null} sapMeta
 */

export {};
