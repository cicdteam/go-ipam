package ipam

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/unrolled/mapstore"
)

type configmap struct {
	manager *mapstore.Manager
	lock    sync.RWMutex
}

// NewConfigmap create a redis storage for ipam
func NewConfigmap(cmName string, cacheInternally bool) Storage {
	return newConfigmap(cmName, cacheInternally)
}
func (cm *configmap) Name() string {
	return "mapstorage"
}

func newConfigmap(cmName string, cacheInternally bool) *configmap {
	ms, _ := mapstore.New(cmName, cacheInternally)
	return &configmap{
		manager: ms,
		lock:    sync.RWMutex{},
	}
}

func (cm *configmap) CreatePrefix(_ context.Context, prefix Prefix) (Prefix, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	_, err := cm.manager.Get(encodeCidr(prefix.Cidr))
	if err == nil {
		return Prefix{}, fmt.Errorf("prefix already created:%v", prefix)
	}

	pfx, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}
	err = cm.manager.Set(encodeCidr(prefix.Cidr), pfx)
	return prefix, err
}

func (cm *configmap) ReadPrefix(_ context.Context, prefix string) (Prefix, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	result, err := cm.manager.Get(encodeCidr(prefix))
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to read existing prefix:%v, error:%w", prefix, err)
	}
	return fromJSON([]byte(result))
}

func (cm *configmap) DeleteAllPrefixes(_ context.Context) error {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	err := cm.manager.Truncate()
	return err
}

func (cm *configmap) ReadAllPrefixes(_ context.Context) (Prefixes, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	encodedCidrs, err := cm.manager.Keys()
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}

	result := Prefixes{}
	for _, cidr := range encodedCidrs {
		p, err := cm.manager.Get(cidr)
		if err != nil {
			return nil, err
		}
		pfx, err := fromJSON(p)
		if err != nil {
			return nil, err
		}
		result = append(result, pfx)
	}

	return result, nil
}
func (cm *configmap) ReadAllPrefixCidrs(_ context.Context) ([]string, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	encodedCidrs, err := cm.manager.Keys()
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}
	decoded := []string{}
	for _, cidr := range encodedCidrs {
		decoded = append(decoded, decodeCidr(cidr))
	}

	return decoded, nil
}
func (cm *configmap) UpdatePrefix(_ context.Context, prefix Prefix) (Prefix, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	oldVersion := prefix.version
	prefix.version = oldVersion + 1

	if prefix.Cidr == "" {
		return Prefix{}, fmt.Errorf("prefix not present:%v", prefix)
	}
	oldPrefix, err := cm.manager.Get(encodeCidr(prefix.Cidr))
	if err != nil {
		return Prefix{}, fmt.Errorf("prefix not found:%s", prefix.Cidr)
	}

	pfx, err := fromJSON(oldPrefix)
	if err != nil {
		return Prefix{}, err
	}

	if pfx.version != oldVersion {
		return Prefix{}, fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
	}

	p, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}

	err = cm.manager.Set(encodeCidr(prefix.Cidr), p)
	return prefix, err
}

func (cm *configmap) DeletePrefix(_ context.Context, prefix Prefix) (Prefix, error) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	err := cm.manager.Delete(encodeCidr(prefix.Cidr))
	if err != nil {
		return *prefix.deepCopy(), err
	}
	return *prefix.deepCopy(), nil
}

// encode string "192.168.100.0/24" to "192_168_100_0-24"
func encodeCidr(cidr string) string {
	return strings.ReplaceAll(strings.ReplaceAll(cidr, ".", "_"), "/", "-")
}

// decode string "192_168_100_0-24" to "192.168.100.0/24"
func decodeCidr(cidr string) string {
	return strings.ReplaceAll(strings.ReplaceAll(cidr, "_", "."), "-", "/")
}
