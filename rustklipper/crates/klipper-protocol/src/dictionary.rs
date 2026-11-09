//! MCU identify dictionary and firmware-defined message codec.
use std::collections::BTreeMap;
use std::io::Read;
use flate2::read::ZlibDecoder;
use serde_json::Value;
use thiserror::Error;
use crate::{decode_i32, encode_i32, encode_u32, ProtocolError};

#[derive(Debug, Error)]
pub enum DictionaryError {
    #[error("invalid identify JSON: {0}")] Json(#[from] serde_json::Error),
    #[error("invalid compressed identify data: {0}")] Compression(#[from] std::io::Error),
    #[error("missing identify field: {0}")] MissingField(&'static str),
    #[error("invalid field {field}: {reason}")] InvalidField { field: String, reason: String },
    #[error("unknown MCU message: {0}")] UnknownMessage(String),
    #[error("missing parameter: {0}")] MissingParameter(String),
    #[error("invalid parameter {name}: {reason}")] InvalidParameter { name: String, reason: String },
    #[error("command format mismatch: {0}")] FormatMismatch(String),
    #[error(transparent)] Protocol(#[from] ProtocolError),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind { Command, Response }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamKind { U32, I32, U16, I16, Byte, String, Progmem, Buffer }
impl ParamKind {
    fn parse(s: &str) -> Option<Self> { Some(match s {
        "%u"=>Self::U32, "%i"=>Self::I32, "%hu"=>Self::U16, "%hi"=>Self::I16,
        "%c"=>Self::Byte, "%s"=>Self::String, "%.*s"=>Self::Progmem, "%*s"=>Self::Buffer,
        _=>return None
    }) }
    fn dynamic(self)->bool { matches!(self,Self::String|Self::Progmem|Self::Buffer) }
    fn signed(self)->bool { matches!(self,Self::I32|Self::I16) }
}
#[derive(Debug, Clone)]
struct Param { name:String, kind:ParamKind, enumeration:Option<String> }
#[derive(Debug, Clone)]
struct Message { id:i32, name:String, format:String, kind:MessageKind, params:Vec<Param> }
#[derive(Debug, Clone, Default)]
struct Enumeration { names:BTreeMap<String,i32>, values:BTreeMap<i32,String> }
impl Enumeration {
    fn add(&mut self,n:String,v:i32){self.values.insert(v,n.clone());self.names.insert(n,v);}
    fn from_json(v:&Value,field:&str)->Result<Self,DictionaryError>{
        let obj=v.as_object().ok_or_else(||invalid_field(field,"expected object"))?;
        let mut e=Self::default();
        for (name,item) in obj {
            if let Some(n)=item.as_i64(){e.add(name.clone(),i32::try_from(n).map_err(|_|invalid_field(field,"enum value outside i32"))?);continue;}
            let a=item.as_array().filter(|a|a.len()==2).ok_or_else(||invalid_field(field,"expected integer or [start,count]"))?;
            let start=i32::try_from(a[0].as_i64().ok_or_else(||invalid_field(field,"range start must be integer"))?).map_err(|_|invalid_field(field,"range start outside i32"))?;
            let count=usize::try_from(a[1].as_u64().ok_or_else(||invalid_field(field,"range count must be unsigned"))?).map_err(|_|invalid_field(field,"range too large"))?;
            let root=name.trim_end_matches(|c:char|c.is_ascii_digit());
            let suffix=&name[root.len()..];
            let first=if suffix.is_empty(){0}else{suffix.parse::<usize>().map_err(|_|invalid_field(field,"bad range suffix"))?};
            for i in 0..count {let value=start.checked_add(i as i32).ok_or_else(||invalid_field(field,"range overflow"))?;e.add(format!("{root}{}",first+i),value);}
        } Ok(e)
    }
}
fn invalid_field(field:&str,reason:&str)->DictionaryError{DictionaryError::InvalidField{field:field.into(),reason:reason.into()}}
fn invalid_param(name:&str,reason:&str)->DictionaryError{DictionaryError::InvalidParameter{name:name.into(),reason:reason.into()}}

#[derive(Debug,Clone,Default)]
pub struct IdentifyDictionary {
    by_name:BTreeMap<String,Message>, by_id:BTreeMap<i32,Message>,
    enums:BTreeMap<String,Enumeration>, constants:BTreeMap<String,String>,
    version:String, build_versions:String, kconfig:Option<Value>, raw_json:Vec<u8>,
}
impl IdentifyDictionary {
    pub fn from_compressed(data:&[u8])->Result<Self,DictionaryError>{
        let mut decoder=ZlibDecoder::new(data);let mut json=Vec::new();decoder.read_to_end(&mut json)?;Self::from_json(&json)
    }
    pub fn from_json(data:&[u8])->Result<Self,DictionaryError>{
        let root:Value=serde_json::from_slice(data)?;
        let obj=root.as_object().ok_or_else(||invalid_field("root","expected object"))?;
        let mut d=Self{raw_json:data.to_vec(),version:obj.get("version").and_then(Value::as_str).unwrap_or("").into(),
            build_versions:obj.get("build_versions").and_then(Value::as_str).unwrap_or("").into(),
            kconfig:obj.get("kconfig").cloned(),..Self::default()};
        if let Some(enums)=obj.get("enumerations"){
            for (name,v) in enums.as_object().ok_or_else(||invalid_field("enumerations","expected object"))?{
                d.enums.insert(name.clone(),Enumeration::from_json(v,&format!("enumerations.{name}"))?);
            }
        }
        let commands=obj.get("commands").and_then(Value::as_object).ok_or(DictionaryError::MissingField("commands"))?;
        let responses=obj.get("responses").and_then(Value::as_object).ok_or(DictionaryError::MissingField("responses"))?;
        for (fmt,id) in commands {d.add_message(fmt,id,MessageKind::Command)?;}
        for (fmt,id) in responses {d.add_message(fmt,id,MessageKind::Response)?;}
        if let Some(config)=obj.get("config").and_then(Value::as_object){
            for (k,v) in config {d.constants.insert(k.clone(),v.as_str().map(str::to_owned).unwrap_or_else(||v.to_string()));}
        }
        Ok(d)
    }
    fn add_message(&mut self,fmt:&str,id:&Value,kind:MessageKind)->Result<(),DictionaryError>{
        let id=i32::try_from(id.as_i64().ok_or_else(||invalid_field(fmt,"message id must be integer"))?).map_err(|_|invalid_field(fmt,"message id outside i32"))?;
        let mut words=fmt.split_whitespace();
        let name=words.next().ok_or_else(||invalid_field("message format","empty"))?.to_owned();
        let mut params=Vec::new();
        for word in words{
            let (p,f)=word.split_once('=').ok_or_else(||invalid_field(&name,"parameter must be name=format"))?;
            let kind=ParamKind::parse(f).ok_or_else(||invalid_field(&name,&format!("unsupported parameter format {f}")))?;
            let enumeration=self.enums.keys().find(|e|p==e.as_str()||p.ends_with(&format!("_{e}"))).cloned();
            params.push(Param{name:p.into(),kind,enumeration});
        }
        let m=Message{id,name:name.clone(),format:fmt.into(),kind,params};
        self.by_id.insert(id,m.clone());self.by_name.insert(name,m);Ok(())
    }
    pub fn version(&self)->&str{&self.version}
    pub fn build_versions(&self)->&str{&self.build_versions}
    pub fn kconfig(&self)->Option<&Value>{self.kconfig.as_ref()}
    pub fn raw_json(&self)->&[u8]{&self.raw_json}
    pub fn constants(&self)->&BTreeMap<String,String>{&self.constants}
    pub fn constant(&self,name:&str)->Option<&str>{self.constants.get(name).map(String::as_str)}
    pub fn constant_i64(&self,name:&str)->Option<i64>{self.constant(name)?.parse().ok()}
    pub fn constant_f64(&self,name:&str)->Option<f64>{self.constant(name)?.parse().ok()}
    pub fn message_id(&self,name:&str)->Option<i32>{self.by_name.get(name).map(|m|m.id)}
    pub fn message_format(&self,name:&str)->Option<&str>{self.by_name.get(name).map(|m|m.format.as_str())}
    pub fn message_kind(&self,name:&str)->Option<MessageKind>{self.by_name.get(name).map(|m|m.kind)}
    pub fn verify_command_format(&self,fmt:&str)->Result<i32,DictionaryError>{
        let name=fmt.split_whitespace().next().unwrap_or("");
        let m=self.by_name.get(name).ok_or_else(||DictionaryError::UnknownMessage(name.into()))?;
        if m.format!=fmt||m.kind!=MessageKind::Command{return Err(DictionaryError::FormatMismatch(fmt.into()));} Ok(m.id)
    }
    pub fn encode_command(&self,name:&str,values:&BTreeMap<String,Value>)->Result<Vec<u8>,DictionaryError>{
        let m=self.by_name.get(name).ok_or_else(||DictionaryError::UnknownMessage(name.into()))?;
        if m.kind!=MessageKind::Command{return Err(invalid_param(name,"not a command"));}
        let mut out=Vec::new();encode_i32(m.id,&mut out);
        for p in &m.params{
            let v=values.get(&p.name).ok_or_else(||DictionaryError::MissingParameter(p.name.clone()))?;
            if p.kind.dynamic(){
                let bytes=if let Some(a)=v.as_array(){a.iter().map(|x|u8::try_from(x.as_u64().ok_or_else(||invalid_param(&p.name,"byte array requires u8 values"))?).map_err(|_|invalid_param(&p.name,"byte outside u8"))).collect::<Result<Vec<_>,_>>()?}
                    else if let Some(s)=v.as_str(){decode_hex(s).map_err(|e|invalid_param(&p.name,&e))?}
                    else{return Err(invalid_param(&p.name,"expected hex string or byte array"));};
                if bytes.len()>255{return Err(invalid_param(&p.name,"buffer exceeds 255 bytes"));}
                out.push(bytes.len() as u8);out.extend(bytes);continue;
            }
            let raw=if let Some(en)=&p.enumeration{
                let key=v.as_str().ok_or_else(||invalid_param(&p.name,"expected enum name"))?;
                *self.enums.get(en).and_then(|e|e.names.get(key)).ok_or_else(||invalid_param(&p.name,&format!("unknown enum value {key}")))?
            }else{
                let n=v.as_i64().ok_or_else(||invalid_param(&p.name,"expected integer"))?;
                match p.kind {
                    ParamKind::U32 => {
                        let n=u32::try_from(n).map_err(|_|invalid_param(&p.name,"expected u32 range"))?;
                        encode_u32(n,&mut out);
                        continue;
                    }
                    ParamKind::U16 => {
                        let n=u16::try_from(n).map_err(|_|invalid_param(&p.name,"expected u16 range"))?;
                        i32::from(n)
                    }
                    ParamKind::Byte => {
                        let n=u8::try_from(n).map_err(|_|invalid_param(&p.name,"expected byte range"))?;
                        i32::from(n)
                    }
                    ParamKind::I16 => i16::try_from(n).map_err(|_|invalid_param(&p.name,"expected i16 range"))? as i32,
                    ParamKind::I32 => i32::try_from(n).map_err(|_|invalid_param(&p.name,"expected i32 range"))?,
                    _ => return Err(invalid_param(&p.name,"unexpected parameter kind")),
                }
            };
            encode_i32(raw,&mut out);
        } Ok(out)
    }
    pub fn decode_message(&self,body:&[u8])->Result<(String,BTreeMap<String,Value>),DictionaryError>{
        let (id,mut pos)=crate::decode_i32(body)?;
        let m=self.by_id.get(&id).ok_or_else(||DictionaryError::UnknownMessage(format!("id {id}")))?;
        let mut result=BTreeMap::new();
        for p in &m.params{
            let value=if p.kind.dynamic(){
                let len=*body.get(pos).ok_or(ProtocolError::TruncatedInteger)? as usize;pos+=1;
                let end=pos.checked_add(len).ok_or(ProtocolError::TruncatedInteger)?;
                let b=body.get(pos..end).ok_or(ProtocolError::TruncatedInteger)?;pos=end;
                Value::Array(b.iter().map(|x|Value::from(*x)).collect())
            }else{
                let (raw,n)=decode_i32(body.get(pos..).ok_or(ProtocolError::TruncatedInteger)?)?;pos+=n;
                if let Some(en)=&p.enumeration{
                    match self.enums.get(en).and_then(|e|e.values.get(&raw)){Some(s)=>Value::String(s.clone()),None=>Value::String(format!("?{raw}"))}
                }else if p.kind.signed(){Value::from(raw)}else{Value::from(raw as u32)}
            };
            result.insert(p.name.clone(),value);
        }
        if pos!=body.len(){return Err(invalid_field(&m.name,&format!("{} trailing bytes",body.len()-pos)));}
        Ok((m.name.clone(),result))
    }
}
fn decode_hex(s:&str)->Result<Vec<u8>,String>{
    let s=s.trim();if s.is_empty(){return Ok(Vec::new());}
    if s.len()%2!=0{return Err("hex string length must be even".into());}
    s.as_bytes().chunks_exact(2).map(|p|{
        let t=std::str::from_utf8(p).map_err(|_|"invalid hex")?;
        u8::from_str_radix(t,16).map_err(|_|format!("invalid hex byte {t}"))
    }).collect()
}

#[cfg(test)]
mod tests{
    use super::*;
    const JSON:&str=r#"{"version":"test-fw","build_versions":"build","enumerations":{"pin":{"PA0":1,"PA1":2},"mode":{"MODE_A":0,"MODE_B":1}},"commands":{"set_pin oid=%c pin=%u value=%c mode=%c":17,"send_data data=%*s":18},"responses":{"uptime high=%u clock=%u":19},"config":{"CLOCK_FREQ":"16000000"}}"#;
    #[test] fn identify_and_enum_codec(){
        let d=IdentifyDictionary::from_json(JSON.as_bytes()).unwrap();assert_eq!(d.version(),"test-fw");assert_eq!(d.constant_i64("CLOCK_FREQ"),Some(16_000_000));
        let mut p=BTreeMap::new();p.insert("oid".into(),Value::from(3));p.insert("pin".into(),Value::from(42));p.insert("value".into(),Value::from(1));p.insert("mode".into(),Value::from("MODE_B"));
        let bytes=d.encode_command("set_pin",&p).unwrap();let (n,v)=d.decode_message(&bytes).unwrap();assert_eq!(n,"set_pin");assert_eq!(v["mode"],"MODE_B");assert_eq!(v["pin"],42);
    }
    #[test] fn dynamic_buffer_codec(){
        let d=IdentifyDictionary::from_json(JSON.as_bytes()).unwrap();let mut p=BTreeMap::new();p.insert("data".into(),Value::from("00a1ff"));
        let b=d.encode_command("send_data",&p).unwrap();assert_eq!(&b[1..],&[3,0,0xa1,0xff]);let (_,v)=d.decode_message(&b).unwrap();assert_eq!(v["data"],serde_json::json!([0,161,255]));
    }
    #[test] fn range_enums_expand(){
        let v=serde_json::json!({"PIN0":[10,3]});let e=Enumeration::from_json(&v,"pin").unwrap();assert_eq!(e.names["PIN2"],12);
    }
    #[test] fn exact_command_format_checked(){
        let d=IdentifyDictionary::from_json(JSON.as_bytes()).unwrap();assert_eq!(d.verify_command_format("set_pin oid=%c pin=%u value=%c mode=%c").unwrap(),17);assert!(d.verify_command_format("set_pin oid=%u").is_err());
    }
}