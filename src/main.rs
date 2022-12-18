use crossbeam_channel::{Receiver, Sender, bounded, SendError, RecvError, IntoIter as CbIntoIter};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use quick_xml::{
	Reader,
	events::{Event, BytesStart},
	events::attributes::AttrError,
	Error as QError
};
use std::{
	collections::HashMap,
	convert::{TryFrom, TryInto},
	error::Error,
	fmt,
	fs::File,
	io::{Read, BufReader},
	num::{ParseIntError, ParseFloatError},
	path::Path,
	str::from_utf8,
	str::{ParseBoolError, Utf8Error},
	string::FromUtf8Error,
	sync::Arc,
	thread::spawn,
};


#[derive(Debug, Clone)]
pub struct ReadError {
	pub msg: String
}

impl Error for ReadError {}

impl fmt::Display for ReadError { fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "Parse error: {}", &self.msg) } }
impl From<AttrError> for ReadError { fn from(e: AttrError) -> Self { Self { msg: format!("Attribute error {:?}", e)} } }
impl From<QError> for ReadError { fn from(e: QError) -> Self { Self { msg: format!("Parsing error {:?}", e)} } }
impl From<ParseIntError> for ReadError { fn from(e: ParseIntError) -> Self { Self { msg: format!("Parsing error {:?}", e)}
 } }
impl From<ParseFloatError> for ReadError { fn from(e: ParseFloatError) -> Self { Self { msg: format!("Parsing error {:?}", e)} } }
impl From<ParseBoolError> for ReadError { fn from(e: ParseBoolError) -> Self { Self { msg: format!("Parsing error {:?}", e)} } }
impl From<FromUtf8Error> for ReadError { fn from(e: FromUtf8Error) -> Self { Self { msg: format!("Parsing error {:?}", e)}
 } }
impl From<Utf8Error> for ReadError { fn from(e: Utf8Error) -> Self { Self { msg: format!("Parsing error {:?}", e)}
 } }

/// An OpenStreetMap object.
#[derive(Debug, Clone)]
pub enum OsmObj {
	/// A node
	Node(Node),
	/// A way
	Way(Way),
	// /// A relation
	Relation(Relation),
}

impl OsmObj {
	pub fn tags_insert(&mut self, k: Arc<str>, v: Arc<str>) {
		match self {
			OsmObj::Node(n) => { n.tags.insert(k, v); }
			OsmObj::Way(n) => { n.tags.insert(k, v); }
			OsmObj::Relation(n) => { n.tags.insert(k, v); }
		}
	}
}

pub type Tags = HashMap<Arc<str>, Arc<str>>;

#[derive(Debug, Clone)]
pub struct OsmElementAttrs {
	pub id: Option<i64>,
	pub timestamp: Option<Arc<str>>,
	pub uid: Option<i64>,
	pub user: Option<Arc<str>>,
	pub visible: Option<bool>,
	pub deleted: Option<bool>,
	pub version: Option<u32>,
	pub changeset: Option<u64>,
}


impl OsmElementAttrs {
	pub fn new() -> Self {
		Self {
			id: None,
			timestamp: None,
			uid: None,
			user: None,
			visible: None,
			deleted: None,
			version: None,
			changeset: None,
		}
	}

	fn _do_push<T: fmt::Display>(&self, elt: &mut BytesStart, key: &str, val: T) {
		elt.push_attribute((key, &val.to_string() as &str));
	}

	pub fn push_to(&self, elt: &mut BytesStart) {
		self._do_push(elt, "id", self.id.unwrap_or(0));
		self._do_push(elt, "timestamp", self.timestamp.as_ref().unwrap_or(&Arc::from("2023-01-01 00:00:00")));
		self._do_push(elt, "uid", self.uid.unwrap_or(0));
		self.user.as_ref().map(|u| self._do_push(elt, "user", u));
		self.visible.map(|v| self._do_push(elt, "visible", v));
		self.deleted.map(|d| self._do_push(elt, "deleted", d));
		self._do_push(elt, "version", self.version.unwrap_or(1));
		self.changeset.map(|c| self._do_push(elt, "changeset", c));
	}
}

//#[derive(Debug)]
pub type ParsedAttrs = HashMap<Arc<str>, Arc<str>>;

// id='27518985' timestamp='2016-09-14T20:53:20Z' uid='1686959' user='_ANick_' visible='true' version='12' changeset='42158932'
#[derive(Debug, Clone)]
pub struct Attrs {
	pub changeset: u64,
	pub deleted: bool,
	pub id: i64,
	pub timestamp: Arc<str>,
	pub uid: i64,
	pub user: Arc<str>,
	pub version: u32,
	pub visible: bool,
}

impl TryFrom<&ParsedAttrs> for OsmElementAttrs {
	type Error = ReadError;
	fn try_from(attrs: &ParsedAttrs) -> Result<Self, Self::Error> {

		Ok(Self {
			changeset: 	attrs.get("changeset")	.map(|v| v.parse::<u64>())	.transpose()?,
			deleted: 	attrs.get("deleted")	.map(|v| v.parse::<bool>())	.transpose()?,
			id: 		attrs.get("id")			.map(|v| v.parse::<i64>())	.transpose()?,
			timestamp: 	attrs.get("timestamp")	.map(|v| v.clone()),
			uid: 		attrs.get("uid")		.map(|v| v.parse::<i64>())	.transpose()?,
			user: 		attrs.get("user")		.map(|v| v.clone()),
			version: 	attrs.get("version")	.map(|v| v.parse::<u32>())	.transpose()?,
			visible: 	attrs.get("visible")	.map(|v| v.parse::<bool>())	.transpose()?,
		})
	}
}



#[derive(Debug, Clone)]
pub struct Way {
	pub attrs: OsmElementAttrs,
	pub nodes: Vec<i64>,
	pub tags: Tags,
}

#[derive(Debug, Clone)]
pub struct Node {
	pub attrs: OsmElementAttrs,
	pub lat: f32,
	pub lon: f32,
	pub tags: Tags
}

#[derive(Debug, Clone)]
pub struct Relation {
	pub attrs: OsmElementAttrs,
	pub tags: Tags,
	pub members: Vec<Member>,
}

#[derive(Debug, Clone)]
pub struct Member {
	pub mtype: ObjType,
	pub mref: i64,
	pub mrole: Arc<str>
}

#[derive(Debug, Clone)]
pub enum ObjType {
	Node, Way, Relation
}

impl<'a> From<ObjType> for &'a str {
	fn from(val: ObjType) -> &'a str {
		match val {
			ObjType::Node => "node",
			ObjType::Way => "way",
			ObjType::Relation => "relation"
		}
	}
}

impl TryFrom<Arc<str>> for ObjType {
	type Error = ReadError;
	fn try_from(val: Arc<str>) -> Result<ObjType, ReadError> {
		match &(*val) {
			"node" => Ok(ObjType::Node),
			"way" => Ok(ObjType::Way),
			"relation" => Ok(ObjType::Relation),
			_ => Err(ReadError { msg: "object type is not node/way/relation".to_string() })
		}
	}
}

pub struct OsmXmlReader {
	pub rd: Reader<BufReader<Box<dyn Read + Send>>>,
	pub elt: Option<OsmObj>,
	pub skip_nodes: bool,
	pub skip_ways: bool,
	pub skip_relations: bool,
	pub curr_elt: Option<ObjType>,
}

type OkOrBox = Result<(), Box<dyn Error>>;
type OkOrBoxStatic = Result<(), Box<dyn Error + 'static>>;
pub type OsmXmlItem = Result<OsmObj, ReadError>;
impl OsmXmlReader {
	pub fn new(rd: BufReader<Box<dyn Read + Send>>) -> OsmXmlReader {
		Self {rd: Reader::from_reader(rd), elt: None, skip_nodes: false, skip_ways: false, skip_relations: false, curr_elt: None }
	}

	pub fn from_path(path: &str) -> Result<OsmXmlReader, Box<dyn Error>> {
		// a wrapper for flat/gzipped/bzipped files
		let fp = File::open(Path::new(&path))?;
		let rd = if path.ends_with(".osm.gz") {
			Box::new(GzDecoder::new(fp)) as Box<dyn Read + Send>
		} else if path.ends_with(".osm.bz2") {
			Box::new(BzDecoder::new(fp)) as Box<dyn Read + Send>
		} else if path.ends_with(".osm") {
			Box::new(fp) as Box<dyn Read + Send>
		} else {
			return Err("file is not .osm format".into());
		};

		Ok(Self::new(BufReader::new(rd)))
	}

	fn _attrs_hashmap(&mut self, elt: &BytesStart) -> Result<ParsedAttrs, ReadError> {
		let mut hm = ParsedAttrs::new();
		for e in elt.attributes() {
			let e = e?;
			let k = from_utf8(e.key)?;
			hm.insert(
				Arc::from(k),
				Arc::from(e.unescape_and_decode_value(&self.rd)?.as_str())
			);
		}
		Ok(hm)
	}

	fn _process_elements(&mut self, elts: Vec<BytesStart>) -> Result<Option<OsmObj>, ReadError> {
		let elt = match elts.first() {
			None => return Ok(None),
			Some(elt) => elt
		};

		let attrs_hashmap = self._attrs_hashmap(elt)?;
		let tags = Tags::new();
		let osm_attrs = OsmElementAttrs::try_from(&attrs_hashmap)?;
		let mut res = match elt.name() {
			b"node" => {
				let lon:f32 = attrs_hashmap.get("lon").ok_or_else(|| ReadError { msg: "node has no longitude".to_string()})?.parse()?;
				let lat:f32 = attrs_hashmap.get("lat").ok_or_else(|| ReadError{ msg: "node has no latitude".to_string()})?.parse()?;
				OsmObj::Node(Node { attrs: osm_attrs, lon: lon, lat: lat, tags: tags })
			},
			b"way" => {
				OsmObj::Way(Way { attrs: osm_attrs, tags: tags, nodes: Vec::new() })
			},
			b"relation" => {
				OsmObj::Relation(Relation { attrs: osm_attrs, tags: tags, members: vec![] })
			}
			x => { panic!("wrong tag on pos 1 in tags vector: {:?}", x) },
		};

		for elt in &elts[1..elts.len()] {
			match elt.name() {
				b"tag" => {
					let hm = self._attrs_hashmap(elt)?;
					let k = hm.get("k");
					let v = hm.get("v");
					if let (Some(k1), Some(v1)) = (k, v) {
						res.tags_insert(k1.clone(), v1.clone());
					}
				},
				b"nd" => {
					if let OsmObj::Way(ref mut w) = res {
						let hm = self._attrs_hashmap(elt)?;
						hm.get("ref").map(|nd| nd.parse::<i64>()).transpose()?.map(|nd| w.nodes.push(nd));
					}
				},
				b"member" => {
					if let OsmObj::Relation(ref mut r) = res {
						let hm = self._attrs_hashmap(elt)?;
						let mtype:ObjType = hm.get("type").ok_or_else(|| ReadError { msg: "member element has no 'type' attribute".to_string() })?.clone().try_into()?;
						let mref:i64 = hm.get("ref").ok_or_else(|| ReadError { msg: "member element has no 'ref' attribute".to_string() })?.parse()?;
						let mrole = hm.get("role").ok_or_else(|| ReadError { msg: "member element has no 'role' attribute".to_string() })?.clone();
						r.members.push(Member { mtype: mtype, mref: mref, mrole: mrole })
					}
				}
				_ => {}
			}
		}

		Ok(Some(res))
	}

	pub fn _next(&mut self) -> Result<Option<OsmObj>, ReadError> {
		let mut buf = Vec::new();
		let mut elements: Vec<BytesStart> = Vec::new();

		let mut obj_started = false;
		let mut do_skip: bool = false;

		loop {
			let e1 = self.rd.read_event(&mut buf);
			match (obj_started, &e1) {
				(_, Err(e)) => { panic!("Error at position {}: {:?}", self.rd.buffer_position(), e) },
				(false, Ok(Event::Start(ref e2)) | Ok(Event::Empty(ref e2))) => {
					let nm = e2.name();
					if matches!(nm, b"nd" | b"tag" | b"member") {
						panic!("nd/tag/member outside of node/way/relation")
					}

					do_skip = match nm {
						b"node" => self.skip_nodes,
						b"way" => self.skip_ways,
						b"relation" => self.skip_relations,
						_ => false
					};

					if matches!(nm, b"node" | b"way" | b"relation") {
						obj_started = true;
						if !do_skip {
							elements.push(e2.to_owned());
						}
						if matches!(e1, Ok(Event::Empty(_))) {
							obj_started = false;
							if !do_skip {
								return self._process_elements(elements);
							}
						}
					}
				},
				(true, Ok(Event::Start(ref e2)) | Ok(Event::Empty(ref e2))) => {
					let nm = e2.name();
					if matches!(nm, b"node" | b"way" | b"relation") {
						panic!("node/way/relation inside another")
					};
					if !do_skip { elements.push(e2.to_owned()); }
				},
				(true, Ok(Event::End(ref e2))) => {
					if do_skip {
						do_skip = false;
						obj_started = false;
						continue;
					}
					if matches!(e2.name(), b"node" | b"way" | b"relation") {
						return self._process_elements(elements);
					}
				},
				(_, Ok(Event::Eof)) => {
					return Ok(None)
				},
				_ => {}
			}

			// if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
			buf.clear();
		}
	}

	pub fn map_nodes<F>(&mut self, mut cb: F) -> OkOrBox
	where F: FnMut(Node) -> OkOrBox {
		self.skip_ways = true;
		self.skip_relations = true;
		for res in self.into_iter() {
			if let OsmObj::Node(n) = res? { cb(n)? }
		}
		Ok(())
	}

	pub fn map_ways<F>(&mut self, mut cb: F) -> OkOrBox
	where F: FnMut(Way) -> OkOrBox {
		self.skip_relations = true;
		self.skip_nodes = true;
		for res in self.into_iter() {
			if let OsmObj::Way(w) = res? { cb(w)? }
		}
		Ok(())
	}

	pub fn in_background(self) -> CbIntoIter<Arc<OsmXmlItem>> {
		let (snd, rec):(Sender<Arc<OsmXmlItem>>, Receiver<Arc<OsmXmlItem>>) = bounded(5);
		spawn(move || -> Result<(), SendError<Arc<OsmXmlItem>>> {
			println!("running in background!");
			for obj in self.into_iter() {
				snd.send(Arc::new(obj))?
			}
			drop(snd);
			Ok(())
		});
		rec.into_iter()
	}

	pub fn map_all<F1, F2, F3>(&mut self, mut node_cb: Option<Box<F1>>, mut way_cb: Option<Box<F2>>, mut rel_cb: Option<Box<F3>>) -> OkOrBoxStatic
		where
			F1: FnMut(Node) -> OkOrBoxStatic,
			F2: FnMut(Way) -> OkOrBoxStatic,
			F3: FnMut(Relation) -> OkOrBoxStatic
	{
		self.skip_nodes = node_cb.is_none();
		self.skip_ways = way_cb.is_none();
		self.skip_relations = rel_cb.is_none();

		// this works fine if it's in the same thread.
		// I tried doing self.in_background() instead and couldn't get the compiler pleased
		for item in self.into_iter() {
			match item? {
				OsmObj::Node(n) => if let Some(ref mut ncb) = node_cb { ncb(n)?; },
				OsmObj::Way(w) => if let Some(ref mut wcb) = way_cb { wcb(w)?; },
				OsmObj::Relation(r) =>  if let Some(ref mut rcb) = rel_cb { rcb(r)?; },
			}
		}
		Ok(())
	}
}

impl Iterator for OsmXmlReader {
	type Item = Result<OsmObj, ReadError>;
	fn next(&mut self) -> Option<<Self as Iterator>::Item> {
		// method _next() was implemented here, because
		// returning Result<Option<T>, E> is much simpler in terms of syntax, than Option<Result<T, E>>
		// (the latter way, you can't use `?` at all).
		self._next().transpose()
		// sorry, methods _next, _parse_items, etc. can't be placed here in `impl Iterator` block
	}
}

fn main() -> Result<(), Box<dyn Error>> {
	let args: Vec<_> = std::env::args_os().collect();
	let (osm_xml_path,) = match args.len() {
		2 => (
			args[1].clone().into_string().expect("broken os string in filename"),
		),
		_ => { return Err("usage: osm_processor INPUT.OSM[.GZ|.BZ2]".into()); }
	};

	let mut nodes_count:i32 = 0;
	let mut ways_count:i32 = 0;
	let mut rels_count:i32 = 0;

	let ncb = |_n: Node| -> OkOrBox { nodes_count +=1; Ok(()) };
	let wcb = |_w: Way| -> OkOrBox{ ways_count +=1; Ok(()) };
	let rcb = |_r: Relation| -> OkOrBox { rels_count +=1; Ok(()) };

	let mut my_rdr = OsmXmlReader::from_path(&osm_xml_path)?;
	my_rdr.map_all(Some(Box::new(ncb)), Some(Box::new(wcb)), None as Option<Box<dyn FnMut(Relation) -> OkOrBoxStatic>>)?;

	// This will not work:
	// my_rdr.map_all(Some(ncb), Some(wcb), None)?;
	// Compiler rejects any kinds of type annotations

	println!("nodes: {}, ways: {}, relations weren't counted", nodes_count, ways_count);
	Ok(())
}
