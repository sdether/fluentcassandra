/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Transport;
namespace Apache.Cassandra
{

  [Serializable]
  public partial class KsDef : TBase
  {
    private string _name;
    private string _strategy_class;
    private Dictionary<string, string> _strategy_options;
    private int _replication_factor;
    private List<CfDef> _cf_defs;

    public string Name
    {
      get
      {
        return _name;
      }
      set
      {
        __isset.name = true;
        this._name = value;
      }
    }

    public string Strategy_class
    {
      get
      {
        return _strategy_class;
      }
      set
      {
        __isset.strategy_class = true;
        this._strategy_class = value;
      }
    }

    public Dictionary<string, string> Strategy_options
    {
      get
      {
        return _strategy_options;
      }
      set
      {
        __isset.strategy_options = true;
        this._strategy_options = value;
      }
    }

    public int Replication_factor
    {
      get
      {
        return _replication_factor;
      }
      set
      {
        __isset.replication_factor = true;
        this._replication_factor = value;
      }
    }

    public List<CfDef> Cf_defs
    {
      get
      {
        return _cf_defs;
      }
      set
      {
        __isset.cf_defs = true;
        this._cf_defs = value;
      }
    }


    public Isset __isset;
    [Serializable]
    public struct Isset {
      public bool name;
      public bool strategy_class;
      public bool strategy_options;
      public bool replication_factor;
      public bool cf_defs;
    }

    public KsDef() {
    }

    public void Read (TProtocol iprot)
    {
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.String) {
              Name = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.String) {
              Strategy_class = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 3:
            if (field.Type == TType.Map) {
              {
                Strategy_options = new Dictionary<string, string>();
                TMap _map29 = iprot.ReadMapBegin();
                for( int _i30 = 0; _i30 < _map29.Count; ++_i30)
                {
                  string _key31;
                  string _val32;
                  _key31 = iprot.ReadString();
                  _val32 = iprot.ReadString();
                  Strategy_options[_key31] = _val32;
                }
                iprot.ReadMapEnd();
              }
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 4:
            if (field.Type == TType.I32) {
              Replication_factor = iprot.ReadI32();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 5:
            if (field.Type == TType.List) {
              {
                Cf_defs = new List<CfDef>();
                TList _list33 = iprot.ReadListBegin();
                for( int _i34 = 0; _i34 < _list33.Count; ++_i34)
                {
                  CfDef _elem35 = new CfDef();
                  _elem35 = new CfDef();
                  _elem35.Read(iprot);
                  Cf_defs.Add(_elem35);
                }
                iprot.ReadListEnd();
              }
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
    }

    public void Write(TProtocol oprot) {
      TStruct struc = new TStruct("KsDef");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (Name != null && __isset.name) {
        field.Name = "name";
        field.Type = TType.String;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Name);
        oprot.WriteFieldEnd();
      }
      if (Strategy_class != null && __isset.strategy_class) {
        field.Name = "strategy_class";
        field.Type = TType.String;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Strategy_class);
        oprot.WriteFieldEnd();
      }
      if (Strategy_options != null && __isset.strategy_options) {
        field.Name = "strategy_options";
        field.Type = TType.Map;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        {
          oprot.WriteMapBegin(new TMap(TType.String, TType.String, Strategy_options.Count));
          foreach (string _iter36 in Strategy_options.Keys)
          {
            oprot.WriteString(_iter36);
            oprot.WriteString(Strategy_options[_iter36]);
            oprot.WriteMapEnd();
          }
        }
        oprot.WriteFieldEnd();
      }
      if (__isset.replication_factor) {
        field.Name = "replication_factor";
        field.Type = TType.I32;
        field.ID = 4;
        oprot.WriteFieldBegin(field);
        oprot.WriteI32(Replication_factor);
        oprot.WriteFieldEnd();
      }
      if (Cf_defs != null && __isset.cf_defs) {
        field.Name = "cf_defs";
        field.Type = TType.List;
        field.ID = 5;
        oprot.WriteFieldBegin(field);
        {
          oprot.WriteListBegin(new TList(TType.Struct, Cf_defs.Count));
          foreach (CfDef _iter37 in Cf_defs)
          {
            _iter37.Write(oprot);
            oprot.WriteListEnd();
          }
        }
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("KsDef(");
      sb.Append("Name: ");
      sb.Append(Name);
      sb.Append(",Strategy_class: ");
      sb.Append(Strategy_class);
      sb.Append(",Strategy_options: ");
      sb.Append(Strategy_options);
      sb.Append(",Replication_factor: ");
      sb.Append(Replication_factor);
      sb.Append(",Cf_defs: ");
      sb.Append(Cf_defs);
      sb.Append(")");
      return sb.ToString();
    }

  }

}
