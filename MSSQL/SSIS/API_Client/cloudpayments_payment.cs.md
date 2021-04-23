```c#
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

[JsonObject]
public class Payment
{
    [JsonProperty("PublicId")]
    public string PublicId { get; set; }

    [JsonProperty("TerminalUrl")]
    public string TerminalUrl { get; set; }

    [JsonProperty("TransactionId")]
    public string TransactionId { get; set; }

    [JsonProperty("Amount")]
    public string Amount { get; set; }

    [JsonProperty("Currency")]
    public string Currency { get; set; }

    [JsonProperty("CurrencyCode")]
    public string CurrencyCode { get; set; }

    [JsonProperty("PaymentAmount")]
    public string PaymentAmount { get; set; }

    [JsonProperty("PaymentCurrency")]
    public string PaymentCurrency { get; set; }

    [JsonProperty("PaymentCurrencyCode")]
    public string PaymentCurrencyCode { get; set; }

    [JsonProperty("InvoiceId")]
    public string InvoiceId { get; set; }

    [JsonProperty("AccountId")]
    public string AccountId { get; set; }

    [JsonProperty("Email")]
    public string Email { get; set; }

    [JsonProperty("Description")]
    public string Description { get; set; }

    [JsonProperty("JsonData")]
    public string JsonData { get; set; }

    [JsonProperty("CreatedDate")]
    public string CreatedDate { get; set; }

    [JsonProperty("PayoutDate")]
    public string PayoutDate { get; set; }

    [JsonProperty("PayoutDateIso")]
    public string PayoutDateIso { get; set; }

    [JsonProperty("PayoutAmount")]
    public string PayoutAmount { get; set; }

    [JsonProperty("CreatedDateIso")]
    public string CreatedDateIso { get; set; }

    [JsonProperty("AuthDate")]
    public string AuthDate { get; set; }

    [JsonProperty("AuthDateIso")]
    public string AuthDateIso { get; set; }

    [JsonProperty("ConfirmDate")]
    public string ConfirmDate { get; set; }

    [JsonProperty("ConfirmDateIso")]
    public string ConfirmDateIso { get; set; }

    [JsonProperty("AuthCode")]
    public string AuthCode { get; set; }

    [JsonProperty("TestMode")]
    public string TestMode { get; set; }

    [JsonProperty("Rrn")]
    public string Rrn { get; set; }

    [JsonProperty("OriginalTransactionId")]
    public string OriginalTransactionId { get; set; }

    [JsonProperty("FallBackScenarioDeclinedTransactionId")]
    public string FallBackScenarioDeclinedTransactionId { get; set; }

    [JsonProperty("IpAddress")]
    public string IpAddress { get; set; }

    [JsonProperty("IpCountry")]
    public string IpCountry { get; set; }

    [JsonProperty("IpCity")]
    public string IpCity { get; set; }

    [JsonProperty("IpRegion")]
    public string IpRegion { get; set; }

    [JsonProperty("IpDistrict")]
    public string IpDistrict { get; set; }

    [JsonProperty("IpLatitude")]
    public string IpLatitude { get; set; }

    [JsonProperty("IpLongitude")]
    public string IpLongitude { get; set; }

    [JsonProperty("CardFirstSix")]
    public string CardFirstSix { get; set; }

    [JsonProperty("CardLastFour")]
    public string CardLastFour { get; set; }

    [JsonProperty("CardExpDate")]
    public string CardExpDate { get; set; }

    [JsonProperty("CardType")]
    public string CardType { get; set; }

    [JsonProperty("CardProduct")]
    public string CardProduct { get; set; }

    [JsonProperty("CardCategory")]
    public string CardCategory { get; set; }

    [JsonProperty("IssuerBankCountry")]
    public string IssuerBankCountry { get; set; }

    [JsonProperty("Issuer")]
    public string Issuer { get; set; }

    [JsonProperty("CardTypeCode")]
    public string CardTypeCode { get; set; }

    [JsonProperty("Status")]
    public string Status { get; set; }

    [JsonProperty("StatusCode")]
    public string StatusCode { get; set; }

    [JsonProperty("CultureName")]
    public string CultureName { get; set; }

    [JsonProperty("Reason")]
    public string Reason { get; set; }

    [JsonProperty("CardHolderMessage")]
    public string CardHolderMessage { get; set; }

    [JsonProperty("Type")]
    public string Type { get; set; }

    [JsonProperty("Refunded")]
    public string Refunded { get; set; }

    [JsonProperty("Name")]
    public string Name { get; set; }

    [JsonProperty("Token")]
    public string Token { get; set; }

    [JsonProperty("SubscriptionId")]
    public string SubscriptionId { get; set; }

    [JsonProperty("IsLocalOrder")]
    public string IsLocalOrder { get; set; }

    [JsonProperty("HideInvoiceId")]
    public string HideInvoiceId { get; set; }

    [JsonProperty("Gateway")]
    public string Gateway { get; set; }

    [JsonProperty("GatewayName")]
    public string GatewayName { get; set; }

    [JsonProperty("ApplePay")]
    public string ApplePay { get; set; }

    [JsonProperty("AndroidPay")]
    public string AndroidPay { get; set; }

    [JsonProperty("MasterPass")]
    public string MasterPass { get; set; }

    [JsonProperty("TotalFee")]
    public string TotalFee { get; set; }

    [JsonProperty("WalletType")]
    public string WalletType { get; set; }

    [JsonProperty("EscrowAccumulationId")]
    public string EscrowAccumulationId { get; set; }

    [JsonProperty("ReasonCode")]
    public string ReasonCode { get; set; }
}
```
