//! Tools for compressing strings
use smaz;
use shoco;
use thiserror::Error;
use std::io::Write;
use std::io::Read;

use crate::TeangaResult;
use crate::document::Document;
use crate::layer::Layer;
use crate::tcf::write::TCFWriteError;

pub trait StringCompression {
    fn compress(&self, input: &str) -> Vec<u8>;
    fn decompress(&self, input: &[u8]) -> StringCompressionResult<String>;
}

#[derive(Error, Debug)]
pub enum StringCompressionError {
    #[error("Smaz Error: {0}")]
    SmazError(#[from] smaz::DecompressError),
    #[error("UTF-8 Error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

pub type StringCompressionResult<T> = Result<T, StringCompressionError>;

pub struct NoCompression;

impl StringCompression for NoCompression {
    fn compress(&self, input: &str) -> Vec<u8> {
        input.as_bytes().to_vec()
    }

    fn decompress(&self, input: &[u8]) -> StringCompressionResult<String> {
        let s = String::from_utf8(input.to_vec())?;
        Ok(s)
    }
}

pub struct SmazCompression;

impl StringCompression for SmazCompression {
    fn compress(&self, input: &str) -> Vec<u8> {
        smaz::compress(input.as_bytes())
    }

    fn decompress(&self, input: &[u8]) -> StringCompressionResult<String> {
        let bytes = smaz::decompress(input)?;
        let s = String::from_utf8(bytes)?;
        Ok(s)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShocoCompression(shoco::ShocoModel);

impl StringCompression for ShocoCompression {
    fn compress(&self, input: &str) -> Vec<u8> {
        shoco::compress(input, &self.0)
    }

    fn decompress(&self, input: &[u8]) -> StringCompressionResult<String> {
        let s = shoco::decompress(input, &self.0)?;
        Ok(s.to_string())
    }
}

impl ShocoCompression {
    pub fn default() -> ShocoCompression {
        ShocoCompression(shoco::ShocoModel::default())
    }

    pub fn from_corpus<'a>(docs : &mut Box<dyn Iterator<Item=TeangaResult<Document>> + 'a>, size : usize) -> Result<ShocoCompression, TCFWriteError> {
        let mut data = Vec::new();
        let mut total_data = 0;
        for doc in docs {
            if total_data > size {
                break;
            }
            for (_, layer) in doc?.into_iter() {
                match layer {
                    Layer::Characters(v) => {
                        let bytes = v.into_bytes();
                        total_data += bytes.len();
                        data.push(bytes);
                    }
                    _ => {}
                }
            }
        }
        let gen_model = shoco::GenShocoModel::from_iter(Box::new(data.into_iter()))
            .generate()?;
        Ok(ShocoCompression(gen_model))

    }
}

pub enum SupportedStringCompression {
    None,
    Smaz,
    Shoco(ShocoCompression),
}

impl StringCompression for SupportedStringCompression {
    fn compress(&self, input: &str) -> Vec<u8> {
        match self {
            SupportedStringCompression::None => NoCompression.compress(input),
            SupportedStringCompression::Smaz => SmazCompression.compress(input),
            SupportedStringCompression::Shoco(c) => c.compress(input),
        }
    }

    fn decompress(&self, input: &[u8]) -> StringCompressionResult<String> {
        match self {
            SupportedStringCompression::None => NoCompression.decompress(input),
            SupportedStringCompression::Smaz => SmazCompression.decompress(input),
            SupportedStringCompression::Shoco(c) => c.decompress(input),
        }
    }
}

pub fn write_shoco_model<W: Write>(out : &mut W, model : &ShocoCompression) -> std::io::Result<()> {
    let model = &model.0;
    out.write(&[model.min_chr])?;
    out.write(&[model.max_chr])?;
    out.write((model.chrs_by_chr_id.len() as u32).to_be_bytes().as_ref())?;
    out.write(&model.chrs_by_chr_id)?;
    for i in 0..256 {
        out.write(&model.chr_ids_by_chr[i].to_be_bytes())?;
    }
    out.write((model.successor_ids_by_chr_id_and_chr_id.len() as u32).to_be_bytes().as_ref())?;
    for s in model.successor_ids_by_chr_id_and_chr_id.iter() {
        out.write((s.len() as u32).to_be_bytes().as_ref())?;
        for i in s.iter() {
            out.write(i.to_be_bytes().as_ref())?;
        }
    }
    out.write((model.chrs_by_chr_and_successor_id.len() as u32).to_be_bytes().as_ref())?;
    for s in model.chrs_by_chr_and_successor_id.iter() {
        out.write((s.len() as u32).to_be_bytes().as_ref())?;
        for i in s.iter() {
            out.write(i.to_be_bytes().as_ref())?;
        }
    }
    out.write((model.packs.len() as u32).to_be_bytes().as_ref())?;
    for p in model.packs.iter() {
        out.write(p.word.to_be_bytes().as_ref())?;
        out.write((p.bytes_packed as u32).to_be_bytes().as_ref())?;
        out.write((p.bytes_unpacked as u32).to_be_bytes().as_ref())?;
        out.write((p.offsets.len() as u32).to_be_bytes().as_ref())?;
        for o in p.offsets.iter() {
            out.write(o.to_be_bytes().as_ref())?;
        }
        assert_eq!(p.offsets.len(), p.masks.len());
        for m in p.masks.iter() {
            out.write(m.to_be_bytes().as_ref())?;
        }
        out.write(&[p.header_mask])?;
        out.write(&[p.header])?;
    }
    out.write((model.max_successor_n as u32).to_be_bytes().as_ref())?;
    Ok(())
}

pub fn read_shoco_model<R: Read>(input : &mut R) -> std::io::Result<ShocoCompression> {
    let mut min_chr_buf = [0; 1];
    input.read_exact(&mut min_chr_buf)?;
    let min_chr = min_chr_buf[0];
    let mut max_chr_buf = [0; 1];
    input.read_exact(&mut max_chr_buf)?;
    let max_chr = max_chr_buf[0];
    let mut chrs_by_chr_id_len_buf = [0; 4];
    input.read_exact(&mut chrs_by_chr_id_len_buf)?;
    let chrs_by_chr_id_len = u32::from_be_bytes(chrs_by_chr_id_len_buf);
    let mut chrs_by_chr_id = Vec::new();
    for _ in 0..chrs_by_chr_id_len {
        let mut chr_buf = [0; 1];
        input.read_exact(&mut chr_buf)?;
        chrs_by_chr_id.push(chr_buf[0]);
    }
    let mut chr_ids_by_chr = [0i8; 256];
    for i in 0..256 {
        let mut chr_id_buf = [0; 1];
        input.read_exact(&mut chr_id_buf)?;
        chr_ids_by_chr[i] = i8::from_be_bytes(chr_id_buf);
    }
    let mut successor_ids_by_chr_id_and_chr_id_len_buf = [0; 4];
    input.read_exact(&mut successor_ids_by_chr_id_and_chr_id_len_buf)?;
    let successor_ids_by_chr_id_and_chr_id_len = u32::from_be_bytes(successor_ids_by_chr_id_and_chr_id_len_buf);
    let mut successor_ids_by_chr_id_and_chr_id = Vec::new();
    for _ in 0..successor_ids_by_chr_id_and_chr_id_len {
        let mut v = Vec::new();
        let mut successor_ids_len_buf = [0; 4];
        input.read_exact(&mut successor_ids_len_buf)?;
        let successor_ids_len = u32::from_be_bytes(successor_ids_len_buf);
        for _ in 0..successor_ids_len {
            let mut successor_id_buf = [0; 1];
            input.read_exact(&mut successor_id_buf)?;
            v.push(i8::from_be_bytes(successor_id_buf));
        }
        successor_ids_by_chr_id_and_chr_id.push(v);
    }
    let mut chrs_by_chr_and_successor_id_len_buf = [0; 4];
    input.read_exact(&mut chrs_by_chr_and_successor_id_len_buf)?;
    let chrs_by_chr_and_successor_id_len = u32::from_be_bytes(chrs_by_chr_and_successor_id_len_buf);
    let mut chrs_by_chr_and_successor_id = Vec::new();
    for _ in 0..chrs_by_chr_and_successor_id_len {
        let mut v = Vec::new();
        let mut chrs_len_buf = [0; 4];
        input.read_exact(&mut chrs_len_buf)?;
        let chrs_len = u32::from_be_bytes(chrs_len_buf);
        for _ in 0..chrs_len {
            let mut chr_buf = [0; 1];
            input.read_exact(&mut chr_buf)?;
            v.push(chr_buf[0]);
        }
        chrs_by_chr_and_successor_id.push(v);
    }
    let mut packs_len_buf = [0; 4];
    input.read_exact(&mut packs_len_buf)?;
    let packs_len = u32::from_be_bytes(packs_len_buf);
    let mut packs = Vec::new();
    for _ in 0..packs_len {
        let mut word_buf = [0; 4];
        input.read_exact(&mut word_buf)?;
        let word = u32::from_be_bytes(word_buf);
        let mut bytes_packed_buf = [0; 4];
        input.read_exact(&mut bytes_packed_buf)?;
        let bytes_packed = u32::from_be_bytes(bytes_packed_buf) as usize;
        let mut bytes_unpacked_buf = [0; 4];
        input.read_exact(&mut bytes_unpacked_buf)?;
        let bytes_unpacked = u32::from_be_bytes(bytes_unpacked_buf) as usize;
        let mut offsets_len_buf = [0; 4];
        input.read_exact(&mut offsets_len_buf)?;
        let offsets_len = u32::from_be_bytes(offsets_len_buf);
        let mut offsets = Vec::new();
        for _ in 0..offsets_len {
            let mut offset_buf = [0; 4];
            input.read_exact(&mut offset_buf)?;
            offsets.push(u32::from_be_bytes(offset_buf));
        }
        let offsets = offsets.try_into().expect("Offset length constant has changed!");
        let mut masks = Vec::new();
        for _ in 0..offsets_len {
            let mut mask_buf = [0; 2];
            input.read_exact(&mut mask_buf)?;
            masks.push(i16::from_be_bytes(mask_buf));
        }
        let masks = masks.try_into().expect("Mask length constant has changed!");
        let mut header_mask_buf = [0; 1];
        input.read_exact(&mut header_mask_buf)?;
        let header_mask = header_mask_buf[0];
        let mut header_buf = [0; 1];
        input.read_exact(&mut header_buf)?;
        let header = header_buf[0];
        packs.push(shoco::Pack {
            word,
            bytes_packed,
            bytes_unpacked,
            offsets,
            masks,
            header_mask,
            header
        });
    }
    let mut max_successor_n_buf = [0; 4];
    input.read_exact(&mut max_successor_n_buf)?;
    let max_successor_n = u32::from_be_bytes(max_successor_n_buf) as usize;
    Ok(ShocoCompression(shoco::ShocoModel {
        min_chr,
        max_chr,
        chrs_by_chr_id,
        chr_ids_by_chr,
        successor_ids_by_chr_id_and_chr_id,
        chrs_by_chr_and_successor_id,
        packs,
        max_successor_n
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Corpus;
    use crate::DataType;
    use crate::SimpleCorpus;
    use crate::layer::LayerType;
    use crate::layer_builder::build_layer;
    use crate::tcf::write::write_tcf_with_config;
    use crate::tcf::read::read_tcf;
    use crate::tcf::TCFConfig;
    use crate::tcf::StringCompressionMethod;

    #[test]
    fn test_shoco_serialize() {
        let mut bytes = Vec::new();

        let model = ShocoCompression(shoco::ShocoModel::default());

        write_shoco_model(&mut bytes, &model).unwrap();

        eprintln!("{:?} bytes", bytes.len());

        let model2 = read_shoco_model(&mut bytes.as_slice()).unwrap();

        assert_eq!(model, model2);
    }

    #[test]
    fn test_no_compression() {
        test_compression(StringCompressionMethod::None);
    }

    #[test]
    fn test_smaz_compression() {
        test_compression(StringCompressionMethod::Smaz);
    }

    #[test]
    fn test_shoco_default_compression() {
        test_compression(StringCompressionMethod::ShocoDefault);
    }

    #[test]
    fn test_shoco_generate_compression() {
        test_compression(StringCompressionMethod::GenerateShocoModel(100));
    }

    fn test_compression(method : StringCompressionMethod) {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        build_layer(&mut corpus, "document")
            .layer_type(LayerType::div)
            .base("characters")
            .default(Layer::L1(vec![0]))
            .add().unwrap();
        build_layer(&mut corpus, "url")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        println!("str: {:?}", smaz::compress(b"Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared."));
        let doc_id = corpus.add_doc(
            vec![("text".to_string(), 
                "Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.".to_string()),
                ("url".to_string(),
                "https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string())
            ]).unwrap();
        let mut doc = corpus.get_doc_by_id(&doc_id).unwrap();
        doc.set("url", Layer::LS(vec!["https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string()]));
        let mut data : Vec<u8> = Vec::new();
        write_tcf_with_config(&mut data, &corpus,
            &TCFConfig::new().with_string_compression(method)).unwrap();
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
        assert_eq!(corpus, corpus2);
    }


}
