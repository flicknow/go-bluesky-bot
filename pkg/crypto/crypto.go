package crypto

import (
	"bytes"
	"encoding/hex"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/crypto"
)

type SigningKey struct {
	signingKey *crypto.PrivateKeyK256
}

func NewSigningKey(signingKeyHex string) (*SigningKey, error) {
	signingKeyBytes := make([]byte, hex.DecodedLen(len(signingKeyHex)))
	_, err := hex.Decode(signingKeyBytes, []byte(signingKeyHex))
	if err != nil {
		return nil, err
	}

	signingKey, err := crypto.ParsePrivateBytesK256(signingKeyBytes)
	if err != nil {
		return nil, err

	}

	return &SigningKey{signingKey: signingKey}, nil
}

func (s *SigningKey) SignLabel(label *atproto.LabelDefs_Label) error {
	label.Sig = nil

	sigBuf := new(bytes.Buffer)
	err := label.MarshalCBOR(sigBuf)
	if err != nil {
		return err
	}

	sigBytes, err := s.signingKey.HashAndSign(sigBuf.Bytes())
	if err != nil {
		return err
	}
	label.Sig = sigBytes

	return nil
}

func (s *SigningKey) SignLabelAndMarshalCBOR(label *atproto.LabelDefs_Label) ([]byte, error) {
	err := s.SignLabel(label)
	if err != nil {
		return nil, err
	}

	cborBuf := new(bytes.Buffer)
	err = label.MarshalCBOR(cborBuf)
	if err != nil {
		return nil, err
	}

	return cborBuf.Bytes(), nil
}
