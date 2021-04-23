```c#
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Newtonsoft.Json.Linq;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    public List<Payment> ListPayments;

    public static List<Payment> GetData(string login, string pass, string date)
    {
        string url = "https://api.cloudpayments.ru/payments/list";
        System.Net.CredentialCache credentialCache = new System.Net.CredentialCache
            {
                {
                    new System.Uri(url),
                    "Basic",
                    new System.Net.NetworkCredential(login, pass)
                }
            };
        string data = "{\"Date\" : \"" + date + "\", \"TimeZone\" : \"MSK\"}";
        byte[] postBytes = Encoding.ASCII.GetBytes(data);

        WebRequest request = WebRequest.Create(url);
        request.Method = "POST";
        request.ContentType = "application/json";
        request.Credentials = credentialCache;
        request.ContentLength = postBytes.Length;
        request.GetRequestStream().Write(postBytes, 0, postBytes.Length);

        // Get the response.
        WebResponse response = request.GetResponse();
        Stream dataStream = response.GetResponseStream();
        StreamReader reader = new StreamReader(dataStream, Encoding.UTF8);
        string result = reader.ReadToEnd();
        response.Close();

        JObject Jresult = JObject.Parse(result);
        List<JToken> Jpayments = Jresult["Model"].Children().ToList();

        List<Payment> payments = new List<Payment>();
        foreach (JToken Jpayment in Jpayments)
        {
            // Deserialization to class Payment structure
            Payment payment = Jpayment.ToObject<Payment>();
            payments.Add(payment);
        }

        return payments;
    }

    public class Cred
    {
        public string login { get; set; }
        public string pass { get; set; }
    }

    public override void PreExecute()
    {
        base.PreExecute();

        //login1 = pk_d9bd6dfb3b5d2324fe90180b2ee67
        //login2 = pk_6275b61397528bfd45f6b93760942
        //login3 = pk_e15776adc4a8465434a82e7ff284e
        //login4 = pk_caedae6431186e7522c8959bce68d
        //login5 = pk_fdc98bccae4edc64043e07d4869b9

        List<Cred> logins = new List<Cred>
        {
            new Cred()
            {
                login = "pk_d9bd6dfb3b5d2324fe90180b2ee67",
                pass = Variables.cloudpaymentspass1
            },
            new Cred()
            {
                login = "pk_6275b61397528bfd45f6b93760942",
                pass = Variables.cloudpaymentspass2
            },
            new Cred()
            {
                login = "pk_e15776adc4a8465434a82e7ff284e",
                pass = Variables.cloudpaymentspass3
            },
            new Cred()
            {
                login = "pk_caedae6431186e7522c8959bce68d",
                pass = Variables.cloudpaymentspass4
            },
            new Cred()
            {
                login = "pk_fdc98bccae4edc64043e07d4869b9",
                pass = Variables.cloudpaymentspass5
            }
        };

        DateTime thisDay = DateTime.Today;
        DateTime lagDay = thisDay.AddDays(-1*Variables.lagday);
        var payments = new List<Payment>();
        while (thisDay.Date >= lagDay.Date)
        {
            foreach (Cred l in logins)
            {
                payments.AddRange(GetData(l.login, l.pass, lagDay.ToString("yyyy-MM-dd")));
            }

            lagDay = lagDay.AddDays(1);
        }

        ListPayments = payments;
    }
 
    public override void PostExecute()
    {
        base.PostExecute();
    }
 
    public override void CreateNewOutputRows()
    {
        foreach (Payment p in ListPayments)
        {
            PaymentsBuffer.AddRow();
            PaymentsBuffer.PublicId = p.PublicId;
            PaymentsBuffer.TerminalUrl = p.TerminalUrl;
            PaymentsBuffer.TransactionId = p.TransactionId;
            PaymentsBuffer.Amount = p.Amount;
            PaymentsBuffer.Currency = p.Currency;
            PaymentsBuffer.CurrencyCode = p.CurrencyCode;
            PaymentsBuffer.PaymentAmount = p.PaymentAmount;
            PaymentsBuffer.PaymentCurrency = p.PaymentCurrency;
            PaymentsBuffer.PaymentCurrencyCode = p.PaymentCurrencyCode;
            PaymentsBuffer.InvoiceId = p.InvoiceId;
            PaymentsBuffer.AccountId = p.AccountId;
            PaymentsBuffer.Email = p.Email;
            PaymentsBuffer.Description = p.Description;
            PaymentsBuffer.JsonData = p.JsonData;
            PaymentsBuffer.CreatedDate = p.CreatedDate;
            PaymentsBuffer.PayoutDate = p.PayoutDate;
            PaymentsBuffer.PayoutDateIso = p.PayoutDateIso;
            PaymentsBuffer.PayoutAmount = p.PayoutAmount;
            PaymentsBuffer.CreatedDateIso = p.CreatedDateIso;
            PaymentsBuffer.AuthDate = p.AuthDate;
            PaymentsBuffer.AuthDateIso = p.AuthDateIso;
            PaymentsBuffer.ConfirmDate = p.ConfirmDate;
            PaymentsBuffer.ConfirmDateIso = p.ConfirmDateIso;
            PaymentsBuffer.AuthCode = p.AuthCode;
            PaymentsBuffer.TestMode = p.TestMode;
            PaymentsBuffer.Rrn = p.Rrn;
            PaymentsBuffer.OriginalTransactionId = p.OriginalTransactionId;
            PaymentsBuffer.FallBackScenarioDeclinedTransactionId = p.FallBackScenarioDeclinedTransactionId;
            PaymentsBuffer.IpAddress = p.IpAddress;
            PaymentsBuffer.IpCountry = p.IpCountry;
            PaymentsBuffer.IpCity = p.IpCity;
            PaymentsBuffer.IpRegion = p.IpRegion;
            PaymentsBuffer.IpDistrict = p.IpDistrict;
            PaymentsBuffer.IpLatitude = p.IpLatitude;
            PaymentsBuffer.IpLongitude = p.IpLongitude;
            PaymentsBuffer.CardFirstSix = p.CardFirstSix;
            PaymentsBuffer.CardLastFour = p.CardLastFour;
            PaymentsBuffer.CardExpDate = p.CardExpDate;
            PaymentsBuffer.CardType = p.CardType;
            PaymentsBuffer.CardProduct = p.CardProduct;
            PaymentsBuffer.CardCategory = p.CardCategory;
            PaymentsBuffer.IssuerBankCountry = p.IssuerBankCountry;
            PaymentsBuffer.Issuer = p.Issuer;
            PaymentsBuffer.CardTypeCode = p.CardTypeCode;
            PaymentsBuffer.Status = p.Status;
            PaymentsBuffer.StatusCode = p.StatusCode;
            PaymentsBuffer.CultureName = p.CultureName;
            PaymentsBuffer.Reason = p.Reason;
            PaymentsBuffer.CardHolderMessage = p.CardHolderMessage;
            PaymentsBuffer.Type = p.Type;
            PaymentsBuffer.Refunded = p.Refunded;
            PaymentsBuffer.Name = p.Name;
            PaymentsBuffer.Token = p.Token;
            PaymentsBuffer.SubscriptionId = p.SubscriptionId;
            PaymentsBuffer.IsLocalOrder = p.IsLocalOrder;
            PaymentsBuffer.HideInvoiceId = p.HideInvoiceId;
            PaymentsBuffer.Gateway = p.Gateway;
            PaymentsBuffer.GatewayName = p.GatewayName;
            PaymentsBuffer.ApplePay = p.ApplePay;
            PaymentsBuffer.AndroidPay = p.AndroidPay;
            PaymentsBuffer.MasterPass = p.MasterPass;
            PaymentsBuffer.TotalFee = p.TotalFee;
            PaymentsBuffer.WalletType = p.WalletType;
            PaymentsBuffer.EscrowAccumulationId = p.EscrowAccumulationId;
            PaymentsBuffer.ReasonCode = p.ReasonCode;
        }
    }
 
}
```