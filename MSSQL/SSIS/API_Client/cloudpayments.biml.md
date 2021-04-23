```biml
<Biml xmlns="http://schemas.varigence.com/biml.xsd">
    <ScriptProjects>
        <ScriptComponentProject ProjectCoreName="SC_2bca370105ff4883a705860bac68cfba" Name="CloudpaymentsExtract">
    <AssemblyReferences>
        <AssemblyReference AssemblyPath="Microsoft.SqlServer.DTSPipelineWrap.dll" />
        <AssemblyReference AssemblyPath="Microsoft.SqlServer.DTSRuntimeWrap.dll" />
        <AssemblyReference AssemblyPath="Microsoft.SqlServer.PipelineHost.dll" />
        <AssemblyReference AssemblyPath="Microsoft.SqlServer.TxScript.dll" />
        <AssemblyReference AssemblyPath="System.dll" />
        <AssemblyReference AssemblyPath="System.Data.dll" />
        <AssemblyReference AssemblyPath="System.Windows.Forms.dll" />
        <AssemblyReference AssemblyPath="System.Xml.dll" />
    </AssemblyReferences>
    <OutputBuffers>
        <OutputBuffer Name="Payments" IsSynchronous="false">
            <Columns>
                <Column Name = "PublicId" DataType = "String" Length = "4000"/>
                <Column Name = "TerminalUrl" DataType = "String" Length = "4000"/>
                <Column Name = "TransactionId" DataType = "String" Length = "4000"/>
                <Column Name = "Amount" DataType = "String" Length = "4000"/>
                <Column Name = "Currency" DataType = "String" Length = "4000"/>
                <Column Name = "CurrencyCode" DataType = "String" Length = "4000"/>
                <Column Name = "PaymentAmount" DataType = "String" Length = "4000"/>
                <Column Name = "PaymentCurrency" DataType = "String" Length = "4000"/>
                <Column Name = "PaymentCurrencyCode" DataType = "String" Length = "4000"/>
                <Column Name = "InvoiceId" DataType = "String" Length = "4000"/>
                <Column Name = "AccountId" DataType = "String" Length = "4000"/>
                <Column Name = "Email" DataType = "String" Length = "4000"/>
                <Column Name = "Description" DataType = "String" Length = "4000"/>
                <Column Name = "JsonData" DataType = "String" Length = "4000"/>
                <Column Name = "CreatedDate" DataType = "String" Length = "4000"/>
                <Column Name = "PayoutDate" DataType = "String" Length = "4000"/>
                <Column Name = "PayoutDateIso" DataType = "String" Length = "4000"/>
                <Column Name = "PayoutAmount" DataType = "String" Length = "4000"/>
                <Column Name = "CreatedDateIso" DataType = "String" Length = "4000"/>
                <Column Name = "AuthDate" DataType = "String" Length = "4000"/>
                <Column Name = "AuthDateIso" DataType = "String" Length = "4000"/>
                <Column Name = "ConfirmDate" DataType = "String" Length = "4000"/>
                <Column Name = "ConfirmDateIso" DataType = "String" Length = "4000"/>
                <Column Name = "AuthCode" DataType = "String" Length = "4000"/>
                <Column Name = "TestMode" DataType = "String" Length = "4000"/>
                <Column Name = "Rrn" DataType = "String" Length = "4000"/>
                <Column Name = "OriginalTransactionId" DataType = "String" Length = "4000"/>
                <Column Name = "FallBackScenarioDeclinedTransactionId" DataType = "String" Length = "4000"/>
                <Column Name = "IpAddress" DataType = "String" Length = "4000"/>
                <Column Name = "IpCountry" DataType = "String" Length = "4000"/>
                <Column Name = "IpCity" DataType = "String" Length = "4000"/>
                <Column Name = "IpRegion" DataType = "String" Length = "4000"/>
                <Column Name = "IpDistrict" DataType = "String" Length = "4000"/>
                <Column Name = "IpLatitude" DataType = "String" Length = "4000"/>
                <Column Name = "IpLongitude" DataType = "String" Length = "4000"/>
                <Column Name = "CardFirstSix" DataType = "String" Length = "4000"/>
                <Column Name = "CardLastFour" DataType = "String" Length = "4000"/>
                <Column Name = "CardExpDate" DataType = "String" Length = "4000"/>
                <Column Name = "CardType" DataType = "String" Length = "4000"/>
                <Column Name = "CardProduct" DataType = "String" Length = "4000"/>
                <Column Name = "CardCategory" DataType = "String" Length = "4000"/>
                <Column Name = "IssuerBankCountry" DataType = "String" Length = "4000"/>
                <Column Name = "Issuer" DataType = "String" Length = "4000"/>
                <Column Name = "CardTypeCode" DataType = "String" Length = "4000"/>
                <Column Name = "Status" DataType = "String" Length = "4000"/>
                <Column Name = "StatusCode" DataType = "String" Length = "4000"/>
                <Column Name = "CultureName" DataType = "String" Length = "4000"/>
                <Column Name = "Reason" DataType = "String" Length = "4000"/>
                <Column Name = "CardHolderMessage" DataType = "String" Length = "4000"/>
                <Column Name = "Type" DataType = "String" Length = "4000"/>
                <Column Name = "Refunded" DataType = "String" Length = "4000"/>
                <Column Name = "Name" DataType = "String" Length = "4000"/>
                <Column Name = "Token" DataType = "String" Length = "4000"/>
                <Column Name = "SubscriptionId" DataType = "String" Length = "4000"/>
                <Column Name = "IsLocalOrder" DataType = "String" Length = "4000"/>
                <Column Name = "HideInvoiceId" DataType = "String" Length = "4000"/>
                <Column Name = "Gateway" DataType = "String" Length = "4000"/>
                <Column Name = "GatewayName" DataType = "String" Length = "4000"/>
                <Column Name = "ApplePay" DataType = "String" Length = "4000"/>
                <Column Name = "AndroidPay" DataType = "String" Length = "4000"/>
                <Column Name = "MasterPass" DataType = "String" Length = "4000"/>
                <Column Name = "TotalFee" DataType = "String" Length = "4000"/>
                <Column Name = "WalletType" DataType = "String" Length = "4000"/>
                <Column Name = "EscrowAccumulationId" DataType = "String" Length = "4000"/>
                <Column Name = "ReasonCode" DataType = "String" Length = "4000"/>
            </Columns>
        </OutputBuffer>
    </OutputBuffers>
    <Files>
        <File Path="AssemblyInfo.cs">using System.Reflection;
using System.Runtime.CompilerServices;

[assembly: AssemblyTitle("SC_2bca370105ff4883a705860bac68cfba.csproj")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Varigence")]
[assembly: AssemblyProduct("SC_2bca370105ff4883a705860bac68cfba.csproj")]
[assembly: AssemblyCopyright("Copyright @ Varigence 2011")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]
[assembly: AssemblyVersion("1.0.*")]</File>
        <File Path="main.cs">
using System;
using System.Data;
using System.Net;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
 
[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    public override void PreExecute()
    {
        base.PreExecute();
    }
 
    public override void PostExecute()
    {
        base.PostExecute();
    }
 
    public override void CreateNewOutputRows()
    {
    }
 
}
 
</File>
    </Files>
    <ReadOnlyVariables>
    </ReadOnlyVariables>
</ScriptComponentProject>
    </ScriptProjects>
    
      <Packages>
         <Package Name="cloudpayments" ProtectionLevel = "EncryptSensitiveWithPassword"> 
            <Tasks>
                <Dataflow Name="CloudpaymentsLoad">
                    <Transformations>
    
                        <ScriptComponentSource Name="CloudpaymentsExtract">
                               <ScriptComponentProjectReference ScriptComponentProjectName="CloudpaymentsExtract"/>
                        </ScriptComponentSource>
    
                    </Transformations>
                </Dataflow>
            </Tasks>
         </Package>
       </Packages>

</Biml>
```