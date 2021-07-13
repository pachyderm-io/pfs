# Set as you wish
DOCKER_ACCOUNT := pachyderm
CONTAINER_NAME := example-joins-inner-outer
CONTAINER_VERSION := 2.0.0-beta.test.3
CONTAINER_TAG := $(DOCKER_ACCOUNT)/$(CONTAINER_NAME):$(CONTAINER_VERSION)

docker-image:
	@docker build --platform=linux/amd64  -t $(CONTAINER_TAG) .
	@docker push $(CONTAINER_TAG)

setup:
	mkdir -p ./stores ./purchases ./returns
	echo "Creating stores..."
	printf '{"storeid":"1", "name":"washington st.","address":{"zipcode":"02108","country":"US"}}' \
	>./stores/STOREID1.txt
	printf '{"storeid":"2", "name":"hollywood bvd.","address":{"zipcode":"02108","country":"US"}}' \
	>./stores/STOREID2.txt
	printf '{"storeid":"3", "name":"beverly hills","address":{"zipcode":"90210","country":"US"}}' \
	>./stores/STOREID3.txt
	printf '{"storeid":"4", "name":"mariposa st.","address":{"zipcode":"94107","country":"US"}}' \
	>./stores/STOREID4.txt
	printf '{"storeid":"5", "name":"tremont st.","address":{"zipcode":"90210","country":"US"}}' \
	>./stores/STOREID5.txt
	echo "Creating purchases..."
	printf "ORDER_HEADER|W080520|2020-10-10\nSKU|1001|QTY|2|UNIT_PRICE|100.00\n" \
	 >./purchases/ORDERW080520_STOREID1.txt
	printf "ORDER_HEADER|W080521|2020-10-10\nSKU|2002|QTY|1|UNIT_PRICE|250.00\n" \
	 >./purchases/ORDERW080521_STOREID1.txt
	printf "ORDER_HEADER|W078929|2020-10-10\nSKU|0615|QTY|1|UNIT_PRICE|75.00\n" \
	 >./purchases/ORDERW078929_STOREID2.txt
	printf "ORDER_HEADER|W598471|2020-10-10\nSKU|1001|QTY|3|UNIT_PRICE|100.00\n" \
	 >./purchases/ORDERW598471_STOREID3.txt
	printf "ORDER_HEADER|W080231|2020-10-10\nSKU|1001|QTY|2|UNIT_PRICE|100.00\n" \
	 >./purchases/ORDERW080231_STOREID5.txt
	printf "ORDER_HEADER|W080528|2020-10-10\nSKU|2002|QTY|1|UNIT_PRICE|250.00\n" \
	 >./purchases/ORDERW080528_STOREID5.txt
	echo "Creating returns..."
	printf "ORDER_HEADER|W080520|2020-10-12\nSKU|1001|QTY|1|UNIT_PRICE|100.00\n" \
	 >./returns/ORDERW080520_STOREID1.txt
	printf "ORDER_HEADER|W080231|2020-10-10\nSKU|1001|QTY|1|UNIT_PRICE|100.00\n" \
	 >./returns/ORDERW080231_STOREID5.txt
	printf "ORDER_HEADER|W080528|2020-10-10\nSKU|2002|QTY|1|UNIT_PRICE|250.00\n" \
	 >./returns/ORDERW080528_STOREID5.txt
	printf "ORDER_HEADER|W261452|2020-10-10\nSKU|0246|QTY|1|UNIT_PRICE|50.00\n" \
	 >./returns/ORDERW261452_STOREID0.txt

create:
	pachctl create repo stores
	pachctl create repo purchases
	pachctl create repo returns
	pachctl put file -r stores@master:/ -f stores
	pachctl list file stores@master
	pachctl put file -r purchases@master:/ -f purchases
	pachctl list file purchases@master
	pachctl put file -r returns@master:/ -f returns
	pachctl list file returns@master	

