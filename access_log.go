package sakurarslog2parquet

import (
	"errors"
	"io/fs"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func WriteAccessLog(parquetFile string, logs []*AccessLog) error {
	f, err := os.Open(parquetFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		f, err = os.Create(parquetFile)
		if err != nil {
			return err
		}
	}
	defer f.Close()

	fields := make(schema.FieldList, 0)
	ts, err := schema.NewPrimitiveNodeLogical("timestamp", parquet.Repetitions.Required, schema.NewTimestampLogicalType(true, schema.TimeUnitMillis), parquet.Types.Int64, 0, -1)
	if err != nil {
		return err
	}
	fields = append(fields, ts)
	fields = append(fields, schema.NewByteArrayNode("host", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("remote_addr", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("identity", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("user", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("method", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("path", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("proto", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewInt32Node("status", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewInt64Node("out_bytes", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("referer", parquet.Repetitions.Required, -1))
	fields = append(fields, schema.NewByteArrayNode("user_agent", parquet.Repetitions.Required, -1))
	schema, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		return err
	}
	w := file.NewParquetWriter(f, schema, file.WithWriterProps(parquet.NewWriterProperties(
		parquet.WithPageIndexEnabled(true),
		parquet.WithCompression(compress.Codecs.Snappy),
	)))
	defer w.Close()
	srgw := w.AppendRowGroup()

	timestamps := make([]int64, 0, len(logs))
	hosts := make([]parquet.ByteArray, 0, len(logs))
	remoteAddrs := make([]parquet.ByteArray, 0, len(logs))
	identities := make([]parquet.ByteArray, 0, len(logs))
	users := make([]parquet.ByteArray, 0, len(logs))
	methods := make([]parquet.ByteArray, 0, len(logs))
	paths := make([]parquet.ByteArray, 0, len(logs))
	protos := make([]parquet.ByteArray, 0, len(logs))
	statuses := make([]int32, 0, len(logs))
	outBytes := make([]int64, 0, len(logs))
	referers := make([]parquet.ByteArray, 0, len(logs))
	userAgents := make([]parquet.ByteArray, 0, len(logs))
	for _, l := range logs {
		timestamps = append(timestamps, l.Time.UnixMilli())
		hosts = append(hosts, parquet.ByteArray(l.Host))
		remoteAddrs = append(remoteAddrs, parquet.ByteArray(l.RemoteAddr))
		identities = append(identities, parquet.ByteArray(l.Identity))
		users = append(users, parquet.ByteArray(l.User))
		methods = append(methods, parquet.ByteArray(l.Method))
		paths = append(paths, parquet.ByteArray(l.Path))
		protos = append(protos, parquet.ByteArray(l.Proto))
		statuses = append(statuses, int32(l.Status))
		outBytes = append(outBytes, int64(l.OutBytes))
		referers = append(referers, parquet.ByteArray(l.Referer))
		userAgents = append(userAgents, parquet.ByteArray(l.UserAgent))
	}
	cw, err := srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.Int64ColumnChunkWriter).WriteBatch(timestamps, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(hosts, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(remoteAddrs, nil, nil); err != nil {
		return err
	}

	// identity
	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(identities, nil, nil); err != nil {
		return err
	}

	// user
	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(users, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(methods, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(paths, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(protos, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.Int32ColumnChunkWriter).WriteBatch(statuses, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.Int64ColumnChunkWriter).WriteBatch(outBytes, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(referers, nil, nil); err != nil {
		return err
	}

	cw, err = srgw.NextColumn()
	if err != nil {
		return err
	}
	if _, err := cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(userAgents, nil, nil); err != nil {
		return err
	}
	return nil
}

type AccessLog struct {
	Host       string
	RemoteAddr string
	Identity   string
	User       string
	Time       time.Time
	Method     string
	Path       string
	Proto      string
	Status     int
	OutBytes   int
	Referer    string
	UserAgent  string
}

var regexpAccessLog = regexp.MustCompile(`(?P<host>\S+)\s(?P<remote>\S+)\s(?P<identity>\S+)\s(?P<user>.*)\s\[(?P<time>.*)\]\s"(?P<method>.*) (?P<path>.*) (?P<proto>.*)"\s(?P<status>\d+)\s(?P<bytes>\S+)\s"(?P<referer>.*)"\s"(?P<useragent>.*)"`)

func ParseAccessLog(line string) (*AccessLog, error) {
	matches := regexpAccessLog.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("not access log")
	}

	host := matches[regexpAccessLog.SubexpIndex("host")]
	remoteAddr := matches[regexpAccessLog.SubexpIndex("remote")]
	identity := matches[regexpAccessLog.SubexpIndex("identity")]
	user := matches[regexpAccessLog.SubexpIndex("user")]
	timestr := matches[regexpAccessLog.SubexpIndex("time")]
	t, err := time.Parse(`02/Jan/2006:15:04:05 -0700`, timestr)
	if err != nil {
		return nil, err
	}
	method := matches[regexpAccessLog.SubexpIndex("method")]
	path := matches[regexpAccessLog.SubexpIndex("path")]
	proto := matches[regexpAccessLog.SubexpIndex("proto")]
	statusstr := matches[regexpAccessLog.SubexpIndex("status")]
	status, err := strconv.Atoi(statusstr)
	if err != nil {
		return nil, err
	}
	bytes := matches[regexpAccessLog.SubexpIndex("bytes")]
	if bytes == "-" {
		bytes = "0"
	}
	outBytes, err := strconv.Atoi(bytes)
	if err != nil {
		return nil, err
	}
	referer := matches[regexpAccessLog.SubexpIndex("referer")]
	userAgent := matches[regexpAccessLog.SubexpIndex("useragent")]

	return &AccessLog{
		Host:       host,
		RemoteAddr: remoteAddr,
		Identity:   identity,
		User:       user,
		Time:       t,
		Method:     method,
		Path:       path,
		Proto:      proto,
		Status:     status,
		OutBytes:   outBytes,
		Referer:    referer,
		UserAgent:  userAgent,
	}, nil
}
