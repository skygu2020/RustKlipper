//! MCU identify dictionary and firmware-defined message codec.
use std::collections::BTreeMap;
use std::io::Read;
use flate2::read::ZlibDecoder;
use serde_json::Value;
use thiserror::Error;
use crate::{decode_i32, decode_u32, encode_i32, encode_u32, ProtocolError};

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
/// Bootstrap response used before the MCU's dynamic dictionary is available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifyChunk {
    pub offset: u32,
    pub data: Vec<u8>,
}

/// Default bootstrap identify response chunk size used by Klipper hosts.
pub const IDENTIFY_CHUNK_SIZE: u8 = 40;

/// Encode the built-in Klipper `identify offset=%u count=%c` request (message id 1).
pub fn encode_identify_request(offset: u32, count: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(7);
    encode_i32(1, &mut out);
    encode_u32(offset, &mut out);
    encode_i32(i32::from(count), &mut out);
    out
}

/// Decode built-in `identify_response offset=%u data=%.*s` (message id 0).
pub fn decode_identify_response(body: &[u8]) -> Result<IdentifyChunk, DictionaryError> {
    let (id, mut pos) = decode_i32(body)?;
    if id != 0 {
        return Err(DictionaryError::UnknownMessage(format!("bootstrap response id {id}, expected 0")));
    }
    let (offset, used) = decode_u32(body.get(pos..).ok_or(ProtocolError::TruncatedInteger)?)?;
    pos += used;
    let len = *body.get(pos).ok_or(ProtocolError::TruncatedInteger)? as usize;
    pos += 1;
    let end = pos.checked_add(len).ok_or(ProtocolError::TruncatedInteger)?;
    let data = body.get(pos..end).ok_or(ProtocolError::TruncatedInteger)?.to_vec();
    pos = end;
    if pos != body.len() {
        return Err(invalid_field("identify_response", "trailing bytes"));
    }
    Ok(IdentifyChunk { offset, data })
}

/// Collects offset-addressed identify_response chunks until the MCU
/// sends an empty chunk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentifyAssembler {
    compressed: Vec<u8>,
    complete: bool,
}

impl IdentifyAssembler {
    pub fn new() -> Self { Self::default() }

    /// Append one chunk. Returns true when the terminating empty chunk arrives.
    pub fn push(&mut self, chunk: IdentifyChunk) -> Result<bool, DictionaryError> {
        if self.complete {
            return Err(invalid_field("identify_response", "data received after completion"));
        }
        let expected = self.next_offset()?;
        if chunk.offset != expected {
            return Err(invalid_field("identify_response",
                &format!("unexpected offset {}, expected {expected}", chunk.offset)));
        }
        if chunk.data.is_empty() {
            self.complete = true;
            return Ok(true);
        }
        self.compressed.extend_from_slice(&chunk.data);
        Ok(false)
    }

    pub fn next_offset(&self) -> Result<u32, DictionaryError> {
        u32::try_from(self.compressed.len())
            .map_err(|_| invalid_field("identify_response", "identify data exceeds u32 offset"))
    }

    pub fn is_complete(&self) -> bool { self.complete }

    pub fn into_dictionary(self) -> Result<IdentifyDictionary, DictionaryError> {
        if !self.complete {
            return Err(invalid_field("identify_response", "identify stream is not complete"));
        }
        IdentifyDictionary::from_compressed(&self.compressed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind { Command, Response, Output }
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
struct Message { id:i32, name:String, format:String, kind:MessageKind, params:Vec<Param>, output:Option<OutputFormat> }
#[derive(Debug, Clone)]
enum OutputPart { Literal(String), Parameter(ParamKind) }
#[derive(Debug, Clone)]
struct OutputFormat { parts:Vec<OutputPart> }
impl OutputFormat {
    fn parse(format:&str)->Result<Self,DictionaryError>{
        const TOKENS:[(&str,ParamKind);8]=[("%.*s",ParamKind::Progmem),("%*s",ParamKind::Buffer),("%hu",ParamKind::U16),("%hi",ParamKind::I16),("%u",ParamKind::U32),("%i",ParamKind::I32),("%c",ParamKind::Byte),("%s",ParamKind::String)];
        let bytes=format.as_bytes(); let mut parts=Vec::new(); let mut literal=String::new(); let mut pos=0;
        while pos<bytes.len(){
            if bytes[pos]!=b'%' { let start=pos; while pos<bytes.len() && bytes[pos]!=b'%' {pos+=1;} literal.push_str(&format[start..pos]); continue; }
            if pos+1<bytes.len() && bytes[pos+1]==b'%' {literal.push('%');pos+=2;continue;}
            let found=TOKENS.iter().find(|(token,_)|format[pos..].starts_with(token));
            let (token,kind)=found.ok_or_else(||invalid_field("output",&format!("invalid output format near {}",&format[pos..])))?;
            if !literal.is_empty(){parts.push(OutputPart::Literal(std::mem::take(&mut literal)));}
            parts.push(OutputPart::Parameter(*kind));pos+=token.len();
        }
        if !literal.is_empty(){parts.push(OutputPart::Literal(literal));}
        Ok(Self{parts})
    }
    fn render(&self,body:&[u8],pos:&mut usize)->Result<String,DictionaryError>{
        let mut out=String::new();
        for part in &self.parts { match part {
            OutputPart::Literal(s)=>out.push_str(s),
            OutputPart::Parameter(kind)=>{
                let value=if kind.dynamic(){
                    let len=*body.get(*pos).ok_or(ProtocolError::TruncatedInteger)? as usize;*pos+=1;
                    let end=pos.checked_add(len).ok_or(ProtocolError::TruncatedInteger)?;
                    let bytes=body.get(*pos..end).ok_or(ProtocolError::TruncatedInteger)?;*pos=end;
                    python_bytes_repr(bytes)
                }else{
                    let (raw,used)=decode_i32(body.get(*pos..).ok_or(ProtocolError::TruncatedInteger)?)?;*pos+=used;
                    if kind.signed(){raw.to_string()}else{(raw as u32).to_string()}
                }; out.push_str(&value);
            }
        }} Ok(out)
    }
}
fn python_bytes_repr(bytes:&[u8])->String{
    let quote=if bytes.contains(&b'\'') && !bytes.contains(&b'"'){b'"'}else{b'\''};
    let mut out=String::from("b");out.push(quote as char);
    for &b in bytes { match b { b'\\'=>out.push_str("\\\\"), b'\n'=>out.push_str("\\n"), b'\r'=>out.push_str("\\r"), b'\t'=>out.push_str("\\t"), x if x==quote=>{out.push('\\');out.push(x as char)}, 0x20..=0x7e=>out.push(b as char), _=>out.push_str(&format!("\\x{b:02x}")) }}
    out.push(quote as char);out
}
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
        if let Some(output)=obj.get("output") {
            for (fmt,id) in output.as_object().ok_or_else(||invalid_field("output","expected object"))? { d.add_output(fmt,id)?; }
        }
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
        let m=Message{id,name:name.clone(),format:fmt.into(),kind,params,output:None};
        self.by_id.insert(id,m.clone());self.by_name.insert(name,m);Ok(())
    }
    fn add_output(&mut self,fmt:&str,id:&Value)->Result<(),DictionaryError>{
        let id=i32::try_from(id.as_i64().ok_or_else(||invalid_field(fmt,"output message id must be integer"))?).map_err(|_|invalid_field(fmt,"output message id outside i32"))?;
        let output=OutputFormat::parse(fmt)?;
        let m=Message{id,name:"#output".into(),format:fmt.into(),kind:MessageKind::Output,params:Vec::new(),output:Some(output)};
        if self.by_id.contains_key(&id){return Err(invalid_field(fmt,"duplicate message id"));}
        self.by_id.insert(id,m); Ok(())
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
    /// Encode a Klipper textual command line using this firmware's identify schema.
    ///
    /// Numeric parameters accept decimal or 0x-prefixed hexadecimal values.
    /// Enumeration parameters are kept as symbolic names. String parameters
    /// are encoded as UTF-8 bytes; progmem/buffer parameters accept hex.
    pub fn encode_command_line(&self, line: &str) -> Result<Vec<u8>, DictionaryError> {
        let mut words = line.split_whitespace();
        let name = words.next().ok_or_else(|| invalid_field("command", "empty command line"))?;
        let message = self.by_name.get(name)
            .ok_or_else(|| DictionaryError::UnknownMessage(name.to_owned()))?;
        if message.kind != MessageKind::Command {
            return Err(invalid_param(name, "not a command"));
        }

        let mut raw_values = BTreeMap::<String, String>::new();
        for word in words {
            let (key, value) = word.split_once('=')
                .ok_or_else(|| invalid_field(name, "expected key=value parameter"))?;
            if raw_values.insert(key.to_owned(), value.to_owned()).is_some() {
                return Err(invalid_param(key, "duplicate parameter"));
            }
        }

        let expected = message.params.iter().map(|p| p.name.as_str()).collect::<std::collections::BTreeSet<_>>();
        for key in raw_values.keys() {
            if !expected.contains(key.as_str()) {
                return Err(invalid_param(key, "unknown parameter"));
            }
        }

        let mut values = BTreeMap::new();
        for param in &message.params {
            let raw = raw_values.get(&param.name)
                .ok_or_else(|| DictionaryError::MissingParameter(param.name.clone()))?;
            let value = if param.enumeration.is_some() {
                Value::String(raw.clone())
            } else {
                match param.kind {
                    ParamKind::String => Value::Array(raw.as_bytes().iter().map(|b| Value::from(*b)).collect()),
                    ParamKind::Progmem | ParamKind::Buffer => Value::String(raw.clone()),
                    _ => {
                        let parsed = if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
                            i64::from_str_radix(hex, 16)
                        } else {
                            raw.parse::<i64>()
                        }.map_err(|_| invalid_param(&param.name, "expected decimal or 0x-prefixed integer"))?;
                        Value::from(parsed)
                    }
                }
            };
            values.insert(param.name.clone(), value);
        }
        self.encode_command(name, &values)
    }

    /// Decode one message and reject trailing bytes.
    pub fn decode_message(&self, body: &[u8]) -> Result<(String, BTreeMap<String, Value>), DictionaryError> {
        let (name, params, used) = self.decode_message_prefix(body)?;
        if used != body.len() {
            return Err(invalid_field(&name, &format!("{} trailing bytes", body.len() - used)));
        }
        Ok((name, params))
    }

    /// Decode every concatenated MCU message in a frame payload.
    pub fn decode_messages(&self, body: &[u8]) -> Result<Vec<(String, BTreeMap<String, Value>)>, DictionaryError> {
        let mut messages = Vec::new();
        let mut offset = 0usize;
        while offset < body.len() {
            let (name, params, used) = self.decode_message_prefix(&body[offset..])?;
            if used == 0 {
                return Err(invalid_field("message", "decoder consumed zero bytes"));
            }
            messages.push((name.clone(), params));
            offset += used;
            // An unknown message has no schema, so its end cannot be inferred.
            if name == "#unknown" { break; }
        }
        if offset != body.len() {
            return Err(invalid_field("message", &format!("{} undecoded trailing bytes", body.len() - offset)));
        }
        Ok(messages)
    }

    fn decode_message_prefix(&self, body: &[u8]) -> Result<(String, BTreeMap<String, Value>, usize), DictionaryError> {
        let (id, mut pos) = crate::decode_i32(body)?;
        let Some(m) = self.by_id.get(&id) else {
            let mut values = BTreeMap::new();
            values.insert("#msgid".into(), Value::from(id));
            values.insert("#msg".into(), Value::Array(body.iter().map(|b| Value::from(*b)).collect()));
            return Ok(("#unknown".into(), values, body.len()));
        };
        if let Some(output) = &m.output {
            let rendered = output.render(body, &mut pos)?;
            let mut values = BTreeMap::new();
            values.insert("#msg".into(), Value::String(rendered));
            return Ok(("#output".into(), values, pos));
        }
        let mut result = BTreeMap::new();
        for p in &m.params {
            let value = if p.kind.dynamic() {
                let len = *body.get(pos).ok_or(ProtocolError::TruncatedInteger)? as usize;
                pos += 1;
                let end = pos.checked_add(len).ok_or(ProtocolError::TruncatedInteger)?;
                let bytes = body.get(pos..end).ok_or(ProtocolError::TruncatedInteger)?;
                pos = end;
                Value::Array(bytes.iter().map(|x| Value::from(*x)).collect())
            } else {
                let (raw, used) = decode_i32(body.get(pos..).ok_or(ProtocolError::TruncatedInteger)?)?;
                pos += used;
                if let Some(en) = &p.enumeration {
                    match self.enums.get(en).and_then(|e| e.values.get(&raw)) {
                        Some(name) => Value::String(name.clone()),
                        None => Value::String(format!("?{raw}")),
                    }
                } else if p.kind.signed() {
                    Value::from(raw)
                } else {
                    Value::from(raw as u32)
                }
            };
            result.insert(p.name.clone(), value);
        }
        Ok((m.name.clone(), result, pos))
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

    #[test]
    fn textual_command_encoder_handles_enums_hex_and_strings() {
        let d = IdentifyDictionary::from_json(br#"{
            "enumerations":{"pin":{"PA0":1,"PA1":2}},
            "commands":{
                "set_pin oid=%c pin=%u value=%u":7,
                "write_text data=%s":8
            },
            "responses":{}
        }"#).unwrap();
        let encoded = d.encode_command_line("set_pin oid=2 pin=PA1 value=0x10").unwrap();
        let mut expected = Vec::new();
        encode_i32(7, &mut expected);
        encode_i32(2, &mut expected);
        encode_u32(2, &mut expected);
        encode_u32(16, &mut expected);
        assert_eq!(encoded, expected);
        let text = d.encode_command_line("write_text data=hello").unwrap();
        assert_eq!(text, vec![8, 5, b'h', b'e', b'l', b'l', b'o']);
        assert!(d.encode_command_line("set_pin oid=2 pin=NOPE value=1").is_err());
        assert!(d.encode_command_line("set_pin oid=2 pin=PA0 value=1 extra=2").is_err());
    }

    #[test]
    fn identify_assembler_checks_offsets_and_terminator() {
        let mut assembler = IdentifyAssembler::new();
        assert!(!assembler.push(IdentifyChunk { offset: 0, data: vec![1, 2, 3] }).unwrap());
        assert_eq!(assembler.next_offset().unwrap(), 3);
        assert!(assembler.push(IdentifyChunk { offset: 2, data: vec![4] }).is_err());
        assert!(assembler.push(IdentifyChunk { offset: 3, data: Vec::new() }).unwrap());
        assert!(assembler.is_complete());
        assert!(assembler.push(IdentifyChunk { offset: 3, data: vec![5] }).is_err());
    }

    #[test]
    fn identify_assembler_decompresses_after_empty_terminator() {
        use flate2::{write::ZlibEncoder, Compression};
        use std::io::Write;
        let json = br#"{"commands":{},"responses":{},"config":{"CLOCK_FREQ":"16000000"}}"#;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(json).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut assembler = IdentifyAssembler::new();
        assert!(assembler.clone().into_dictionary().is_err());
        let mut offset = 0u32;
        for part in compressed.chunks(7) {
            assert!(!assembler.push(IdentifyChunk { offset, data: part.to_vec() }).unwrap());
            offset += part.len() as u32;
        }
        assert!(assembler.push(IdentifyChunk { offset, data: Vec::new() }).unwrap());
        assert_eq!(assembler.into_dictionary().unwrap().constant("CLOCK_FREQ"), Some("16000000"));
    }

    #[test] fn output_format_parses_literals_escapes_and_typed_values(){
        let json=r#"{"commands":{},"responses":{},"output":{"mcu: clock=%u signed=%i text=%.*s percent=%%":5}}"#;
        let d=IdentifyDictionary::from_json(json.as_bytes()).unwrap();
        let mut body=Vec::new();encode_i32(5,&mut body);encode_u32(16000000,&mut body);encode_i32(-7,&mut body);body.push(2);body.extend_from_slice(b"ok");
        let (name,values)=d.decode_message(&body).unwrap();
        assert_eq!(name,"#output");assert_eq!(values["#msg"],"mcu: clock=16000000 signed=-7 text=b'ok' percent=%");
    }
    #[test] fn unknown_message_is_preserved_for_diagnostics(){
        let d=IdentifyDictionary::from_json(br#"{"commands":{},"responses":{}}"#).unwrap();
        let mut body=Vec::new();encode_i32(77,&mut body);body.extend_from_slice(&[0xaa,0xbb]);
        let (name,values)=d.decode_message(&body).unwrap();
        assert_eq!(name,"#unknown");assert_eq!(values["#msgid"],77);
        assert_eq!(values["#msg"],serde_json::json!([77,170,187]));
    }
    #[test] fn output_format_rejects_unknown_conversion(){
        let json=r#"{"commands":{},"responses":{},"output":{"bad %q":5}}"#;
        assert!(IdentifyDictionary::from_json(json.as_bytes()).is_err());
    }

    #[test] fn bootstrap_identify_request_and_response() {
        let request = encode_identify_request(300, 40);
        assert_eq!(decode_i32(&request).unwrap().0, 1);
        let mut response = Vec::new();
        encode_i32(0, &mut response);
        encode_u32(300, &mut response);
        response.push(3);
        response.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        let chunk = decode_identify_response(&response).unwrap();
        assert_eq!(chunk, IdentifyChunk { offset: 300, data: vec![0xaa, 0xbb, 0xcc] });
    }

    #[test] fn identify_and_enum_codec(){
        let d=IdentifyDictionary::from_json(JSON.as_bytes()).unwrap();assert_eq!(d.version(),"test-fw");assert_eq!(d.constant_i64("CLOCK_FREQ"),Some(16_000_000));
        let mut p=BTreeMap::new();p.insert("oid".into(),Value::from(3));p.insert("pin".into(),Value::from("PA1"));p.insert("value".into(),Value::from(1));p.insert("mode".into(),Value::from("MODE_B"));
        let bytes=d.encode_command("set_pin",&p).unwrap();let (n,v)=d.decode_message(&bytes).unwrap();assert_eq!(n,"set_pin");assert_eq!(v["mode"],"MODE_B");assert_eq!(v["pin"],"PA1");
    }
    #[test]
    fn decodes_multiple_concatenated_messages_in_one_frame() {
        let d = IdentifyDictionary::from_json(JSON.as_bytes()).unwrap();
        let mut body = Vec::new();
        for (high, clock) in [(1, 100), (2, 200)] {
            encode_i32(19, &mut body);
            encode_u32(high, &mut body);
            encode_u32(clock, &mut body);
        }
        let messages = d.decode_messages(&body).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "uptime");
        assert_eq!(messages[0].1["high"], 1);
        assert_eq!(messages[0].1["clock"], 100);
        assert_eq!(messages[1].1["high"], 2);
        assert_eq!(messages[1].1["clock"], 200);
        assert!(d.decode_message(&body).is_err());
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